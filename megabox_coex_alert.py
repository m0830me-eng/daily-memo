import os
import json
import time
import html
import hashlib
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curl_cffi import requests


# ============================================================
# MEGABOX COEX MONITOR
# 감지 대상:
# - 메가토크 (GV / 관객과의 대화 포함)
# - 무대인사
# - DOLBY CINEMA
#
# 코엑스 지점번호: 1351
#
# 최종 감시 방식:
# - 한국시간 기준 오늘~49일 뒤 = 50일 전체
# - 50일 전체를 한 사이클로 검사
# - 2 workers 고정
# - 모든 API 요청 시작 간격 최소 0.17초
# - 한 사이클 종료 후 즉시 다음 50일 전체 스캔
# ============================================================

BRANCH_NO = "1351"
BRANCH_NAME = "메가박스 코엑스"

DAYS = 50
WORKERS = 2
REQUEST_GAP = 0.17
OVERLOAD_GAP_MAX = 0.35
OVERLOAD_COOLDOWN_SECONDS = 30.0
OVERLOAD_RECOVERY_CYCLES = 10
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))

KST = ZoneInfo("Asia/Seoul")

SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

STATE_FILE = "seen_megabox_coex.json"
STATUS_FILE = "status_megabox_coex.json"
BASELINE_FILE = "baseline_megabox_coex.done"
EVENT_PAGE_URL = "https://www.megabox.co.kr/event"
OFFICIAL_EVENT_CHECK_INTERVAL = 30.0
HEARTBEAT_INTERVAL = 600.0  # 10분마다 Actions 요약 로그

# 기존 14일 baseline을 자동으로 무효화해서
# 업그레이드 첫 실행 때 50일 전체를 조용히 새 기준값으로 등록한다.
BASELINE_SCHEMA = "MEGABOX_COEX_50DAYS_GAP017_STATUS_EVENT_V3"

DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_MEGABOX_COEX",
    "",
).strip()

DISCORD_USER_ID = os.environ.get(
    "DISCORD_USER_ID",
    "",
).strip()

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": (
        "https://www.megabox.co.kr/"
        "theater/time?brchNo=1351"
    ),
    "Origin": "https://www.megabox.co.kr",
    "X-Requested-With": "XMLHttpRequest",
}

BLOCK_STATUSES = {
    403, 429, 500, 502, 503, 504,
}

_thread_local = threading.local()
_rate_lock = threading.Lock()
_next_request_time = 0.0
_adaptive_lock = threading.Lock()
_active_request_gap = REQUEST_GAP
_overload_until = 0.0
_overload_events = 0
_clean_cycle_streak = 0


# ============================================================
# Time
# ============================================================

def now_kst():
    return datetime.now(KST)


def make_dates():
    today = now_kst()

    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    ]


def pretty_date(date):
    dt = datetime.strptime(date, "%Y%m%d")

    weekdays = [
        "월", "화", "수", "목",
        "금", "토", "일",
    ]

    return (
        f"{dt.year}.{dt.month}.{dt.day}"
        f"({weekdays[dt.weekday()]})"
    )


# ============================================================
# Discord
# ============================================================

def send_discord(message):
    if not DISCORD_WEBHOOK:
        print("WEBHOOK MISSING")
        return False

    payload = {
        "content": message,
        "flags": 4,  # Discord 링크 미리보기(Embed) 숨김
    }

    if DISCORD_USER_ID:
        payload["allowed_mentions"] = {
            "users": [DISCORD_USER_ID]
        }

    try:
        response = requests.post(
            DISCORD_WEBHOOK,
            json=payload,
            impersonate="chrome",
            timeout=15,
        )

        response.raise_for_status()

        print(
            "DISCORD SENT:",
            response.status_code,
        )

        return True

    except Exception as e:
        print(
            "DISCORD ERROR:",
            repr(e),
        )
        return False


# ============================================================
# State
# ============================================================

def load_seen():
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(
            STATE_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        return set(data) if isinstance(data, list) else set()

    except Exception as e:
        print(
            "STATE LOAD ERROR:",
            repr(e),
        )
        return set()


def save_seen(seen):
    try:
        with open(
            STATE_FILE,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                sorted(seen),
                f,
                ensure_ascii=False,
                indent=2,
            )

    except Exception as e:
        print(
            "STATE SAVE ERROR:",
            repr(e),
        )


def load_status():
    if not os.path.exists(STATUS_FILE):
        return {
            "shows": {},
            "official_events": {},
        }

    try:
        with open(
            STATUS_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("status root is not dict")

        shows = data.get("shows")
        official_events = data.get("official_events")

        return {
            "shows": shows if isinstance(shows, dict) else {},
            "official_events": (
                official_events
                if isinstance(official_events, dict)
                else {}
            ),
        }

    except Exception as e:
        print("STATUS LOAD ERROR:", repr(e))
        return {
            "shows": {},
            "official_events": {},
        }


def save_status(status):
    try:
        with open(
            STATUS_FILE,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                status,
                f,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

    except Exception as e:
        print("STATUS SAVE ERROR:", repr(e))


def baseline_done():
    if not os.path.exists(BASELINE_FILE):
        return False

    try:
        with open(
            BASELINE_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            value = f.read().strip()

        return value == BASELINE_SCHEMA

    except Exception:
        return False


def mark_baseline_done():
    with open(
        BASELINE_FILE,
        "w",
        encoding="utf-8",
    ) as f:
        f.write(BASELINE_SCHEMA)

    print("BASELINE MARKER CREATED")


# ============================================================
# API / rate helpers
# ============================================================

def get_session():
    session = getattr(
        _thread_local,
        "session",
        None,
    )

    if session is None:
        session = requests.Session(
            impersonate="chrome"
        )
        _thread_local.session = session

    return session


def reset_thread_session():
    try:
        _thread_local.session = None
    except Exception:
        pass


def reset_rate_clock():
    global _next_request_time

    with _rate_lock:
        _next_request_time = time.monotonic()


def current_request_gap():
    with _adaptive_lock:
        return _active_request_gap


def overload_event_count():
    with _adaptive_lock:
        return _overload_events


def register_overload(reason):
    global _active_request_gap, _overload_until, _overload_events, _clean_cycle_streak

    now = time.monotonic()
    should_log = False

    with _adaptive_lock:
        _overload_events += 1
        _clean_cycle_streak = 0

        # 같은 과부하 파동에서 두 worker가 동시에 실패해도
        # 간격을 여러 번 올리거나 로그를 도배하지 않는다.
        if now >= _overload_until:
            _active_request_gap = min(
                OVERLOAD_GAP_MAX,
                max(0.22, _active_request_gap + 0.03),
            )
            _overload_until = now + OVERLOAD_COOLDOWN_SECONDS
            should_log = True

    if should_log:
        print(
            "⚠️ 메가박스 서버 과부하 감지 -> "
            f"{OVERLOAD_COOLDOWN_SECONDS:.0f}초 자동 휴식 / "
            f"요청간격 {current_request_gap():.2f}s로 완화 | "
            f"원인: {reason}"
        )


def note_clean_cycle():
    global _active_request_gap, _clean_cycle_streak

    changed = None
    with _adaptive_lock:
        _clean_cycle_streak += 1
        if (
            _clean_cycle_streak >= OVERLOAD_RECOVERY_CYCLES
            and _active_request_gap > REQUEST_GAP
        ):
            _active_request_gap = max(
                REQUEST_GAP,
                round(_active_request_gap - 0.01, 2),
            )
            _clean_cycle_streak = 0
            changed = _active_request_gap

    if changed is not None:
        print(
            "✅ 과부하 없이 10사이클 완료 -> "
            f"요청간격 {changed:.2f}s로 한 단계 복구"
        )


def wait_rate_slot():
    global _next_request_time

    while True:
        with _adaptive_lock:
            overload_until = _overload_until
            gap = _active_request_gap

        with _rate_lock:
            now = time.monotonic()
            target = max(_next_request_time, overload_until)

            if now < target:
                sleep_for = target - now
            else:
                _next_request_time = now + gap
                return

        time.sleep(sleep_for)


def extract_movie_form_list(data):
    mega_map = data.get("megaMap") or {}
    rows = mega_map.get("movieFormList")

    if isinstance(rows, list):
        return rows

    for value in data.values():
        if not isinstance(value, dict):
            continue

        rows = value.get("movieFormList")

        if isinstance(rows, list):
            return rows

    return []


def request_schedule(
    date,
    special=False,
):
    if special:
        params = {
            "masterType": "brch",
            "detailType": "spcl",
            "theabKindCd": "DBC",
            "brchNo": BRANCH_NO,
            "firstAt": "N",
            "brchNo1": BRANCH_NO,
            "spclbYn1": "Y",
            "theabKindCd1": "DBC",
            "crtDe": now_kst().strftime("%Y%m%d"),
            "playDe": date,
        }
        label = "DOLBY"

    else:
        params = {
            "masterType": "brch",
            "detailType": "movie",
            "brchNo": BRANCH_NO,
            "firstAt": "N",
            "brchNo1": BRANCH_NO,
            "spclbYn1": "N",
            "crtDe": now_kst().strftime("%Y%m%d"),
            "playDe": date,
        }
        label = "GENERAL"

    last_error = ""

    for attempt in range(1, 3):
        wait_rate_slot()
        started = time.monotonic()
        session = get_session()

        try:
            response = session.post(
                SCHEDULE_API,
                data=params,
                headers=HEADERS,
                timeout=8,
            )

        except Exception as e:
            last_error = (
                f"{label} {date} ERROR {repr(e)}"
            )
            reset_thread_session()

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None, True, last_error

        elapsed = time.monotonic() - started

        if response.status_code in BLOCK_STATUSES:
            last_error = (
                f"{label} {date} "
                f"HTTP={response.status_code}"
            )
            register_overload(f"HTTP {response.status_code}")
            reset_thread_session()

            if attempt < 2:
                continue

            return None, True, last_error

        if response.status_code != 200:
            return None, True, (
                f"{label} {date} "
                f"HTTP={response.status_code}"
            )

        response_preview = (
            response.text[:160]
            .replace("\n", " ")
            .replace("\r", " ")
        )

        if "Workload is so high" in response_preview:
            last_error = (
                f"{label} {date} SERVER OVERLOAD "
                f"PREVIEW={response_preview!r}"
            )
            register_overload("Workload is so high")
            reset_thread_session()

            if attempt < 2:
                continue

            return None, True, last_error

        try:
            data = response.json()
            rows = extract_movie_form_list(data)

            retry_text = (
                f" ATTEMPT={attempt}"
                if attempt > 1
                else ""
            )

            return rows, False, (
                f"{label} {date} "
                f"HTTP=200 {elapsed:.2f}s "
                f"ROWS={len(rows)}"
                f"{retry_text}"
            )

        except Exception as e:
            preview = (
                response.text[:120]
                .replace("\n", " ")
                .replace("\r", " ")
            )

            last_error = (
                f"{label} {date} JSON ERROR "
                f"{repr(e)} PREVIEW={preview!r}"
            )

            register_overload("HTTP 200 비정상 JSON")
            reset_thread_session()

            # HTTP 200이어도 비정상/빈 응답이면 한 번 새 세션으로 재시도.
            # 과부하 보호가 전역 휴식/간격 완화를 담당한다.
            if attempt < 2:
                continue

            return None, True, last_error

    return None, True, (
        f"{label} {date} UNKNOWN ERROR"
    )


# ============================================================
# Row helpers / classification
# ============================================================

def all_text(row):
    values = []

    for key, value in row.items():
        if value is None:
            continue

        values.append(
            f"{key}={value}"
        )

    return " ".join(values)


def is_stage(row):
    text = all_text(row)

    return (
        "무대인사" in text
        or "舞台挨拶" in text
    )


def is_megatalk(row):
    text = all_text(row)
    compact = re.sub(r"\s+", "", text)
    upper = text.upper()

    if "메가토크" in compact:
        return True

    if "관객과의대화" in compact:
        return True

    # 메가박스가 행사명을 GV로만 쓰는 경우도 메가토크로 통합.
    if re.search(r"(?<![A-Z0-9])GV(?![A-Z0-9])", upper):
        return True

    return False


def is_dolby(row):
    fields = [
        "playKindNm",
        "playKindName",
        "theabExpoNm",
        "theabNm",
        "theabKindCd",
        "theabKindCdNm",
        "spclbYn",
    ]

    text = " ".join(
        str(row.get(field) or "")
        for field in fields
    ).upper()

    return (
        "DBC" in text
        or "DOLBY" in text
        or "돌비" in text
    )


def get_target_type(
    row,
    from_dolby=False,
):
    if is_stage(row):
        return "무대인사"

    if is_megatalk(row):
        return "메가토크"

    if from_dolby or is_dolby(row):
        return "DOLBY"

    return None


# ============================================================
# Event fields
# ============================================================

def clean_text(value):
    return html.unescape(
        str(value or "")
    ).strip()


def get_movie(row):
    return clean_text(
        row.get("movieNm")
        or row.get("movNm")
        or ""
    )


def get_start(row):
    return clean_text(
        row.get("playStartTime")
        or row.get("scnsrtTm")
        or ""
    )


def get_end(row):
    return clean_text(
        row.get("playEndTime")
        or row.get("scnsEndTime")
        or ""
    )


def get_screen(row):
    return clean_text(
        row.get("theabExpoNm")
        or row.get("theabNm")
        or row.get("screenNm")
        or row.get("scnsNm")
        or ""
    )


def get_schedule_no(row):
    return clean_text(
        row.get("playSchdlNo")
        or row.get("playScheduleNo")
        or ""
    )


def make_booking_link(row):
    schedule_no = get_schedule_no(row)

    if schedule_no:
        return (
            "https://m.megabox.co.kr/"
            "booking/seat"
            f"?playSchdlNo={schedule_no}"
        )

    date = clean_text(
        row.get("playDe")
        or ""
    )

    if not date:
        date = now_kst().strftime("%Y%m%d")

    return (
        "https://www.megabox.co.kr/"
        "theater/time"
        f"?brchNo={BRANCH_NO}"
        f"&playDe={date}"
    )


def event_key(
    date,
    row,
    event_type,
):
    schedule_no = get_schedule_no(row)

    if schedule_no:
        return "|".join([
            BRANCH_NO,
            date,
            schedule_no,
            event_type,
        ])

    return "|".join([
        BRANCH_NO,
        date,
        clean_text(
            row.get("movieNo")
            or ""
        ),
        get_movie(row),
        get_start(row),
        get_end(row),
        get_screen(row),
        event_type,
    ])


def to_int(value):
    try:
        if value is None or value == "":
            return None
        return int(str(value).replace(",", "").strip())
    except Exception:
        return None


def get_rest_seat(row):
    for key in (
        "restSeatCnt",
        "restSeatCount",
        "remainSeatCnt",
        "remainSeatCount",
    ):
        value = to_int(row.get(key))
        if value is not None:
            return value
    return None


def get_total_seat(row):
    for key in (
        "totSeatCnt",
        "totSeatCount",
        "theabSeatCnt",
        "totalSeatCnt",
    ):
        value = to_int(row.get(key))
        if value is not None:
            return value
    return None


def booking_status(event):
    # 메가박스 원본에 실제 "매진" 문구가 있으면 좌석수 필드보다 우선한다.
    # (사용자 화면/응답 형식이 달라져도 매진 -> 재오픈을 놓치지 않기 위함)
    if event.get("sold_out_explicit"):
        return "SOLD_OUT"

    rest = event.get("rest_seat")

    if isinstance(rest, int):
        return "SOLD_OUT" if rest <= 0 else "OPEN"

    return "UNKNOWN"


def normalize_event(
    date,
    row,
    event_type,
):
    row_text = all_text(row)
    row_upper = row_text.upper()
    sold_out_explicit = (
        "매진" in row_text
        or "SOLD_OUT" in row_upper
        or "SOLDOUT" in row_upper
    )

    return {
        "date": date,
        "type": event_type,
        "movie": get_movie(row),
        "start": get_start(row),
        "end": get_end(row),
        "screen": get_screen(row),
        "schedule_no": get_schedule_no(row),
        "link": make_booking_link(row),
        "rest_seat": get_rest_seat(row),
        "total_seat": get_total_seat(row),
        "sold_out_explicit": sold_out_explicit,
    }


# ============================================================
# Official event page early signal (best effort)
# ============================================================

def compact_ws(text):
    return re.sub(r"\s+", " ", clean_text(text)).strip()


def strip_tags(text):
    text = re.sub(
        r"(?is)<(script|style).*?>.*?</\1>",
        " ",
        text,
    )
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return compact_ws(html.unescape(text))


def classify_event_text(text):
    compact = re.sub(r"\s+", "", text)
    upper = text.upper()

    if "무대인사" in compact or "舞台挨拶" in text:
        return "무대인사"

    if (
        "메가토크" in compact
        or "관객과의대화" in compact
        or re.search(r"(?<![A-Z0-9])GV(?![A-Z0-9])", upper)
    ):
        return "메가토크"

    return None


def fetch_official_event_signals():
    """공식 이벤트 페이지 HTML 안에서 코엑스 관련 선행 신호를 찾는다.

    메가박스 이벤트 목록은 일부가 동적으로 로드될 수 있어서 이 소스는
    '선행 보조 신호'로만 사용한다. 실제 회차 확정은 schedulePage.do가 담당한다.
    """
    try:
        session = requests.Session(impersonate="chrome")
        response = session.get(
            EVENT_PAGE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9",
                "Referer": "https://www.megabox.co.kr/",
            },
            timeout=10,
        )

        if response.status_code != 200:
            print(
                "OFFICIAL EVENT PAGE:",
                response.status_code,
                "- skip",
            )
            return None

        raw = response.text
        visible = strip_tags(raw)
        signals = {}

        # eventNo가 HTML 안에 있으면 그것을 가장 안정적인 키로 쓴다.
        event_no_matches = list(
            re.finditer(r"eventNo=(\d+)", raw, flags=re.I)
        )

        # 키워드 주변에 '코엑스'가 같이 있을 때만 코엑스 선행 신호로 인정.
        keyword_re = re.compile(
            r"무대인사|舞台挨拶|메가토크|관객\s*과의\s*대화|(?<![A-Z0-9])GV(?![A-Z0-9])",
            flags=re.I,
        )

        for match in keyword_re.finditer(visible):
            start = max(0, match.start() - 350)
            end = min(len(visible), match.end() + 350)
            context = compact_ws(visible[start:end])

            if "코엑스" not in context:
                continue

            event_type = classify_event_text(context)
            if event_type is None:
                continue

            # 너무 긴 문맥은 알림/키 안정성을 위해 축약.
            snippet = context[:500]

            # 원본 HTML에서 eventNo를 안정적으로 연결하기 어렵다면
            # 문맥 해시를 사용한다. 같은 페이지 내용이면 중복되지 않는다.
            signature = hashlib.sha1(
                (event_type + "|" + snippet).encode("utf-8")
            ).hexdigest()

            signals[signature] = {
                "type": event_type,
                "snippet": snippet,
                "url": EVENT_PAGE_URL,
                "detected_at_kst": now_kst().isoformat(timespec="seconds"),
            }

        return signals

    except Exception as e:
        print("OFFICIAL EVENT PAGE ERROR:", repr(e))
        return None


def send_official_signal(signal):
    lines = []

    if DISCORD_USER_ID:
        lines.extend([
            f"<@{DISCORD_USER_ID}>",
            "",
        ])

    event_type = signal.get("type") or "이벤트"

    lines.extend([
        f"**📣 메가박스 코엑스 · {event_type} 공식 이벤트 선행 감지**",
        "",
        "메가박스 공식 이벤트 페이지에서 코엑스 관련 신호가 확인됐습니다.",
        "아직 실제 상영 회차가 시간표에 나오지 않았을 수 있습니다.",
        "",
        f"🔎 {signal.get('snippet', '')}",
        f"🎟️ {signal.get('url', EVENT_PAGE_URL)}",
    ])

    return send_discord("\n".join(lines))


def process_official_signals(signals, official_state):
    if signals is None:
        return 0

    sent = 0

    for key, signal in signals.items():
        if key in official_state:
            continue

        if send_official_signal(signal):
            official_state[key] = signal
            sent += 1

    return sent


# ============================================================
# Collect - 50 days / 2 workers / fixed 0.17s request gap
# ============================================================

def collect_date(
    index,
    date,
):
    events = {}
    problem = False
    logs = []

    general_rows, general_problem, general_log = (
        request_schedule(
            date,
            special=False,
        )
    )
    logs.append(general_log)
    problem = problem or general_problem

    if general_rows is None:
        general_rows = []

    for row in general_rows:
        event_type = get_target_type(
            row,
            from_dolby=False,
        )

        if event_type not in (
            "메가토크",
            "무대인사",
        ):
            continue

        key = event_key(
            date,
            row,
            event_type,
        )

        events[key] = normalize_event(
            date,
            row,
            event_type,
        )

    dolby_rows, dolby_problem, dolby_log = (
        request_schedule(
            date,
            special=True,
        )
    )
    logs.append(dolby_log)
    problem = problem or dolby_problem

    if dolby_rows is None:
        dolby_rows = []

    for row in dolby_rows:
        event_type = get_target_type(
            row,
            from_dolby=True,
        )

        if event_type is None:
            continue

        key = event_key(
            date,
            row,
            event_type,
        )

        events[key] = normalize_event(
            date,
            row,
            event_type,
        )

    return {
        "index": index,
        "date": date,
        "events": events,
        "problem": problem,
        "logs": logs,
    }


def collect_all_50_days(progress=False):
    all_events = {}
    failed_dates = []
    results = []
    dates = make_dates()

    reset_rate_clock()

    with ThreadPoolExecutor(
        max_workers=WORKERS
    ) as executor:
        futures = [
            executor.submit(
                collect_date,
                index,
                date,
            )
            for index, date in enumerate(
                dates,
                start=1,
            )
        ]

        completed = 0
        for future in as_completed(futures):
            try:
                results.append(
                    future.result()
                )
            except Exception as e:
                print(
                    "DATE FUTURE ERROR:",
                    repr(e),
                )
            finally:
                completed += 1
                if progress and completed in {10, 20, 30, 40, 50}:
                    print(
                        f"⏳ 기준값 진행: {completed}/50 날짜 처리 완료"
                    )

    results.sort(
        key=lambda item: item["index"]
    )

    returned_dates = {
        item["date"]
        for item in results
    }

    for date in dates:
        if date not in returned_dates:
            failed_dates.append(date)

    error_examples = []

    for item in results:
        all_events.update(
            item["events"]
        )

        if item["problem"]:
            failed_dates.append(
                item["date"]
            )
            if len(error_examples) < 3:
                error_examples.append(
                    f"{item['date']}: " + " | ".join(item["logs"])
                )

    # 오류가 많아도 50개 날짜 로그를 전부 찍지 않는다.
    if error_examples:
        print(
            "⚠️ API 오류 예시(최대 3건): "
            + " || ".join(error_examples)
        )

    # 중복 실패 날짜 제거 + 날짜순 정렬
    failed_dates = sorted(
        set(failed_dates)
    )

    return all_events, failed_dates


# ============================================================
# Discord / booking-state transitions
# ============================================================

def state_record(event, status=None):
    current_status = status or booking_status(event)
    return {
        "status": current_status,
        "date": event.get("date", ""),
        "type": event.get("type", ""),
        "movie": event.get("movie", ""),
        "start": event.get("start", ""),
        "end": event.get("end", ""),
        "screen": event.get("screen", ""),
        "schedule_no": event.get("schedule_no", ""),
        "rest_seat": event.get("rest_seat"),
        "total_seat": event.get("total_seat"),
        "sold_out_explicit": bool(event.get("sold_out_explicit")),
        "updated_at_kst": now_kst().isoformat(timespec="seconds"),
    }


def send_new_events(events, seen):
    new_events = [
        (key, event)
        for key, event in events.items()
        if key not in seen
    ]

    if not new_events:
        return 0, set()

    groups = {}

    for key, event in new_events:
        group_key = (
            event["date"],
            event["type"],
        )
        groups.setdefault(group_key, []).append((key, event))

    sent_count = 0
    sent_keys = set()

    for (date, event_type), items in sorted(groups.items()):
        display_type = (
            "DOLBY CINEMA"
            if event_type == "DOLBY"
            else event_type
        )

        title = (
            f"🎬 메가박스 코엑스 · "
            f"{display_type} 새 상영 일정"
        )

        lines = []
        if DISCORD_USER_ID:
            lines.extend([
                f"<@{DISCORD_USER_ID}>",
                "",
            ])

        lines.extend([
            f"**{title}**",
            "",
            f"📅 **{pretty_date(date)}**",
        ])

        items.sort(
            key=lambda x: (
                x[1]["start"],
                x[1]["movie"],
            )
        )

        for key, event in items:
            start = event["start"]
            end = event["end"]
            time_text = f"{start}~{end}" if end else start
            movie = event["movie"]
            screen = event["screen"]
            linked_movie = f"[{movie}]({event['link']})"

            seat = ""
            if isinstance(event.get("rest_seat"), int):
                seat = f" · 잔여 {event['rest_seat']}"
                if isinstance(event.get("total_seat"), int):
                    seat += f"/{event['total_seat']}"

            if screen:
                lines.append(
                    f"🎟️ {time_text} · {linked_movie} · {screen}{seat}"
                )
            else:
                lines.append(
                    f"🎟️ {time_text} · {linked_movie}{seat}"
                )

        print(
            "NEW EVENT GROUP:",
            event_type,
            date,
            "COUNT=",
            len(items),
        )

        if send_discord("\n".join(lines)):
            for key, _ in items:
                seen.add(key)
                sent_keys.add(key)
                sent_count += 1

    return sent_count, sent_keys


def send_cancel_ticket(event):
    display_type = (
        "DOLBY CINEMA"
        if event.get("type") == "DOLBY"
        else event.get("type")
    )

    lines = []
    if DISCORD_USER_ID:
        lines.extend([
            f"<@{DISCORD_USER_ID}>",
            "",
        ])

    start = event.get("start", "")
    end = event.get("end", "")
    time_text = f"{start}~{end}" if end else start
    movie = event.get("movie") or "영화명 확인 필요"
    screen = event.get("screen") or "상영관 정보 없음"
    rest = event.get("rest_seat")
    total = event.get("total_seat")

    seat_text = ""
    if isinstance(rest, int):
        seat_text = f"\n💺 잔여 {rest}"
        if isinstance(total, int):
            seat_text += f" / {total}"

    lines.extend([
        f"**🎟️ 메가박스 코엑스 · {display_type} 취소표가 나타났습니다**",
        f"📅 {pretty_date(event['date'])}",
        f"🎟️ {time_text} · [{movie}]({event['link']}) · {screen}{seat_text}",
    ])

    return send_discord("\n".join(lines))


def process_booking_states(events, show_state):
    cancel_sent = 0
    sold_out_new = 0

    for key, event in events.items():
        current = booking_status(event)
        previous_record = show_state.get(key) or {}
        previous = previous_record.get("status")

        # 처음 본 회차는 현재 상태만 저장.
        # 새 상영 일정 알림 자체는 seen 로직이 별도로 담당한다.
        if previous is None:
            show_state[key] = state_record(event, current)
            continue

        if current == "SOLD_OUT":
            if previous != "SOLD_OUT":
                sold_out_new += 1
                print(
                    "SOLD OUT STORED:",
                    event.get("type"),
                    event.get("movie"),
                    event.get("date"),
                    event.get("start"),
                )

            record = state_record(event, "SOLD_OUT")
            record["sold_out_since_kst"] = (
                previous_record.get("sold_out_since_kst")
                if previous == "SOLD_OUT"
                else now_kst().isoformat(timespec="seconds")
            )
            show_state[key] = record
            continue

        if current == "OPEN" and previous == "SOLD_OUT":
            if send_cancel_ticket(event):
                cancel_sent += 1
                show_state[key] = state_record(event, "OPEN")
            else:
                # 전송 실패면 SOLD_OUT 상태를 유지해 다음 사이클에 재시도.
                continue
            continue

        # 응답에서 좌석 필드가 일시적으로 빠진 UNKNOWN은 기존 상태를 지우지 않는다.
        # 특히 SOLD_OUT 기억이 UNKNOWN으로 덮이면 다음 OPEN 때 취소표를 놓칠 수 있다.
        if current == "UNKNOWN":
            if previous is None:
                show_state[key] = state_record(event, "UNKNOWN")
            else:
                preserved = state_record(event, previous)
                if previous_record.get("sold_out_since_kst"):
                    preserved["sold_out_since_kst"] = previous_record["sold_out_since_kst"]
                show_state[key] = preserved
            continue

        # OPEN 등 일반 상태 갱신
        show_state[key] = state_record(event, current)

    return cancel_sent, sold_out_new


# ============================================================
# Diagnostics
# ============================================================

def count_events(events):
    counts = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    for event in events.values():
        event_type = event.get("type", "")
        if event_type in counts:
            counts[event_type] += 1

    return counts


def print_counts(events):
    counts = count_events(events)

    print("MEGATALK COUNT:", counts["메가토크"])
    print("무대인사 COUNT:", counts["무대인사"])
    print("DOLBY COUNT:", counts["DOLBY"])


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 72)
    print("MEGABOX COEX MONITOR")
    print("=" * 72)

    print(
        "BRANCH:",
        BRANCH_NAME,
    )

    print(
        "BRANCH NO:",
        BRANCH_NO,
    )

    print(
        "TARGET: "
        "메가토크(GV 포함) / 무대인사 / DOLBY CINEMA"
    )

    print(
        "DATE RANGE: "
        "TODAY ~ +49 DAYS (50 DAYS TOTAL)"
    )

    print(
        "SCAN MODE: "
        "50 DAYS FULL SCAN / 2 WORKERS"
    )

    print(
        "REQUEST START GAP:",
        f"{REQUEST_GAP:.2f}s START / overload auto-backoff",
    )

    print(
        "NEXT CYCLE: "
        "IMMEDIATELY AFTER EACH 50-DAY SCAN"
    )

    print(
        "EARLY SIGNAL: 공식 이벤트 페이지 / "
        "메가토크(GV·관객과의 대화 포함) / 무대인사"
    )
    print(
        "REOPEN: 매진 또는 잔여 0 -> 좌석 재발생 시 "
        "'취소표가 나타났습니다'"
    )
    print(
        "LOG MODE: 기준값 10/20/30/40/50 진행 + "
        "정상 감시는 10분 요약 / 서버 과부하는 즉시 자동완화"
    )

    print(
        "RUN SECONDS:",
        RUN_SECONDS,
    )

    print(
        "KST NOW:",
        now_kst().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    )

    print("=" * 72)

    try:
        session = requests.Session(
            impersonate="chrome"
        )

        r = session.get(
            "https://www.megabox.co.kr/",
            headers=HEADERS,
            timeout=8,
        )

        print(
            "MEGABOX PAGE STATUS:",
            r.status_code,
        )

    except Exception as e:
        print(
            "MEGABOX PAGE CHECK WARNING:",
            repr(e),
        )
        print(
            "MAIN PAGE CHECK FAILED - "
            "CONTINUE MONITORING"
        )

    # --------------------------------------------------------
    # 최초/업그레이드 기준값
    # --------------------------------------------------------

    if not baseline_done():
        print()
        print("=" * 72)
        print("INITIAL 50-DAY BASELINE")
        print("=" * 72)
        print(
            "현재 50일 전체 메가토크(GV 포함) / 무대인사 / DOLBY와 "
            "공식 이벤트 선행 신호를 알림 없이 기준값으로 등록합니다."
        )

        events, failed_dates = (
            collect_all_50_days(progress=True)
        )

        if failed_dates:
            print(
                "BASELINE FAILED DATES:",
                ", ".join(
                    failed_dates
                ),
            )
            print(
                "불완전한 기준값은 저장하지 않습니다."
            )
            return

        seen = set(
            events.keys()
        )

        official_signals = fetch_official_event_signals()
        if official_signals is None:
            official_signals = {}

        status = {
            "shows": {
                key: state_record(event)
                for key, event in events.items()
            },
            "official_events": official_signals,
        }

        print(
            "BASELINE EVENT COUNT:",
            len(seen),
        )
        print(
            "BASELINE OFFICIAL SIGNAL COUNT:",
            len(official_signals),
        )

        print_counts(events)

        save_seen(seen)
        save_status(status)
        mark_baseline_done()

        print("BASELINE COMPLETE")
        print(
            "이번 기준값 등록에서는 "
            "Discord 알림을 보내지 않았습니다."
        )
        print(
            "✅ 기준값 등록 완료 - 이 실행을 종료하지 않고 "
            "다음 정규 자동실행 1분 전까지 계속 감시합니다."
        )

    # --------------------------------------------------------
    # 정상 감시
    # --------------------------------------------------------

    seen = load_seen()
    status = load_status()
    show_state = status["shows"]
    official_state = status["official_events"]
    monitor_started = time.monotonic()
    cycle_number = 0
    last_official_event_check = 0.0

    heartbeat_started = monitor_started
    heartbeat_cycles = 0
    heartbeat_new = 0
    heartbeat_cancel = 0
    heartbeat_soldout = 0
    heartbeat_official = 0
    heartbeat_failed_dates = 0
    latest_counts = {"메가토크": 0, "무대인사": 0, "DOLBY": 0}

    while True:
        total_elapsed = (
            time.monotonic()
            - monitor_started
        )

        if total_elapsed >= RUN_SECONDS:
            print("MONITOR TIME FINISHED")
            break

        cycle_number += 1
        overload_before = overload_event_count()

        events, failed_dates = collect_all_50_days()
        cycle_overloads = overload_event_count() - overload_before

        # 완전 정상 사이클일 때만 현재 개수를 최신값으로 교체한다.
        # 서버 과부하로 50일이 비어도 메가토크/무대인사/DOLBY가 0으로 보이지 않게 한다.
        if not failed_dates:
            latest_counts = count_events(events)
            if cycle_overloads == 0:
                note_clean_cycle()

        if failed_dates:
            print(
                f"⚠️ 이번 사이클 API 실패: {len(failed_dates)}/50 날짜 | "
                f"현재 요청간격 {current_request_gap():.2f}s"
            )

        new_count, _sent_keys = send_new_events(
            events,
            seen,
        )

        cancel_count, sold_out_count = process_booking_states(
            events,
            show_state,
        )

        official_count = 0
        now_mono = time.monotonic()
        if (
            now_mono - last_official_event_check
            >= OFFICIAL_EVENT_CHECK_INTERVAL
        ):
            official_signals = fetch_official_event_signals()
            official_count = process_official_signals(
                official_signals,
                official_state,
            )
            last_official_event_check = now_mono

        save_seen(seen)
        save_status(status)

        heartbeat_cycles += 1
        heartbeat_new += new_count
        heartbeat_cancel += cancel_count
        heartbeat_soldout += sold_out_count
        heartbeat_official += official_count
        heartbeat_failed_dates += len(failed_dates)

        now_mono = time.monotonic()
        if now_mono - heartbeat_started >= HEARTBEAT_INTERVAL:
            mins = int((now_mono - heartbeat_started) // 60)
            health = (
                "💚 정상 감시중"
                if heartbeat_failed_dates == 0
                else "⚠️ 감시중(일부 API 오류)"
            )
            print(
                f"{health} | "
                f"최근 {mins}분 {heartbeat_cycles}사이클 완료 | "
                f"누적 CYCLE #{cycle_number} | "
                f"메가토크 {latest_counts['메가토크']} | "
                f"무대인사 {latest_counts['무대인사']} | "
                f"DOLBY {latest_counts['DOLBY']} | "
                f"새 일정 {heartbeat_new} | "
                f"매진변화 {heartbeat_soldout} | "
                f"취소표 {heartbeat_cancel} | "
                f"공식선행 {heartbeat_official} | "
                f"API실패 {heartbeat_failed_dates}건 | "
                f"현재간격 {current_request_gap():.2f}s"
            )

            heartbeat_started = now_mono
            heartbeat_cycles = 0
            heartbeat_new = 0
            heartbeat_cancel = 0
            heartbeat_soldout = 0
            heartbeat_official = 0
            heartbeat_failed_dates = 0

    save_seen(seen)
    save_status(status)

    print(
        "FINAL SEEN STATE:",
        len(seen),
    )

    print("DONE")


if __name__ == "__main__":
    main()
