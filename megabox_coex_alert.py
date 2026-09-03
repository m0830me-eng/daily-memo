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
# - 한국시간 기준 오늘~42일 뒤 = 43일 전체 감시
# - GENERAL / DOLBY 모두 같은 날짜 구간 주기로 분산 감시
# - 오늘(0일) 20초 / 내일(+1일) 20초 / +2~+4일 90초
# - +5~+14일 30초 / +15~+30일 60초 / +31~+42일 300초
# - 매시 00분/30분: +4~+21일 18일을 2 workers + 0.17초로 빠른 전체점검
# - 2 workers 고정, 모든 API 요청 시작 간격 최소 0.17초
# - timeout은 7초 + 새 세션 0.5초 후 1회 즉시 재시도
# - 실제 403/429/503만 전역 과부하 보호
# - 정상 감시는 10분마다 💚 heartbeat 요약
# ============================================================

BRANCH_NO = "1351"
BRANCH_NAME = "메가박스 코엑스"

DAYS = 43
WORKERS = 2
REQUEST_GAP = 0.17
OVERLOAD_GAP_STEPS = (0.30, 0.40, 0.50)
OVERLOAD_COOLDOWN_STEPS = (60.0, 120.0, 300.0)
OVERLOAD_RECOVERY_CYCLES = 10

# 네트워크 timeout은 서버 과부하와 분리한다.
SCHEDULE_TIMEOUT = 7.0
MAIN_PAGE_TIMEOUT = 4.0
OFFICIAL_EVENT_TIMEOUT = 4.0
CONNECTION_RETRY_DELAY = 0.5
TRUE_OVERLOAD_STATUSES = {403, 429, 503}
TRANSIENT_HTTP_STATUSES = {500, 502, 504}

# GitHub Actions workflow에서 RUN_SECONDS=9247을 넣으면 9247초 실행된다.
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
OFFICIAL_EVENT_CHECK_INTERVAL = 120.0
HEARTBEAT_INTERVAL = 600.0  # 10분마다 Actions 요약 로그

# GENERAL / DOLBY 공통 날짜별 감시 주기
INTERVAL_0_1 = 20.0
INTERVAL_2_4 = 90.0
INTERVAL_5_14 = 30.0
INTERVAL_15_30 = 60.0
INTERVAL_31_42 = 300.0
MAX_DUE_DATES_PER_BATCH = 2

# 정각/30분 빠른 전체점검: +4~+21일 = 18일
SAFETY_SCAN_START_OFFSET = 4
SAFETY_SCAN_END_OFFSET = 21
SAFETY_SCAN_EVERY_MINUTES = 30

# 기존 baseline과 구분
BASELINE_SCHEMA = "MEGABOX_COEX_43DAYS_STAGGERED_0030_V1"

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

_thread_local = threading.local()
_rate_lock = threading.Lock()
_next_request_time = 0.0
_adaptive_lock = threading.Lock()
_active_request_gap = REQUEST_GAP
_overload_until = 0.0
_overload_events = 0
_overload_level = 0
_clean_cycle_streak = 0
_cycle_abort_event = threading.Event()


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


def make_safety_scan_dates():
    today = now_kst()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(SAFETY_SCAN_START_OFFSET, SAFETY_SCAN_END_OFFSET + 1)
    ]


def next_halfhour_boundary(dt):
    """Return the next KST :00 or :30 boundary strictly after dt."""
    base = dt.replace(second=0, microsecond=0)
    if dt.minute < 30:
        return base.replace(minute=30)
    return base.replace(minute=0) + timedelta(hours=1)


def pretty_date(date):
    dt = datetime.strptime(date, "%Y%m%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return f"{dt.year}.{dt.month}.{dt.day}({weekdays[dt.weekday()]})"


# ============================================================
# Discord
# ============================================================

def send_discord(message):
    if not DISCORD_WEBHOOK:
        print("WEBHOOK MISSING")
        return False

    payload = {
        "content": message,
        "flags": 4,
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
        print("DISCORD SENT:", response.status_code)
        return True
    except Exception as e:
        print("DISCORD ERROR:", repr(e))
        return False


# ============================================================
# State
# ============================================================

def load_seen():
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except Exception as e:
        print("STATE LOAD ERROR:", repr(e))
        return set()


def save_seen(seen):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                sorted(seen),
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print("STATE SAVE ERROR:", repr(e))


def load_status():
    if not os.path.exists(STATUS_FILE):
        return {
            "shows": {},
            "official_events": {},
        }

    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
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
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
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
        with open(BASELINE_FILE, "r", encoding="utf-8") as f:
            value = f.read().strip()
        return value == BASELINE_SCHEMA
    except Exception:
        return False


def mark_baseline_done():
    with open(BASELINE_FILE, "w", encoding="utf-8") as f:
        f.write(BASELINE_SCHEMA)
    print("BASELINE MARKER CREATED")


# ============================================================
# API / rate helpers
# ============================================================

def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session(impersonate="chrome")
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


def cycle_aborted():
    return _cycle_abort_event.is_set()


def reset_cycle_abort():
    _cycle_abort_event.clear()


def register_overload(reason):
    global _active_request_gap, _overload_until, _overload_events
    global _overload_level, _clean_cycle_streak

    now = time.monotonic()
    should_log = False
    cooldown = 0.0

    # 실제 과부하가 감지되면 현재 조회 묶음은 중단한다.
    _cycle_abort_event.set()

    with _adaptive_lock:
        _overload_events += 1
        _clean_cycle_streak = 0

        # 같은 과부하 파동에서 worker들이 거의 동시에 실패해도
        # 단계/로그는 한 번만 올린다.
        if now >= _overload_until:
            _overload_level = min(
                len(OVERLOAD_GAP_STEPS),
                _overload_level + 1,
            )
            index = _overload_level - 1
            _active_request_gap = OVERLOAD_GAP_STEPS[index]
            cooldown = OVERLOAD_COOLDOWN_STEPS[index]
            _overload_until = now + cooldown
            should_log = True

    if should_log:
        print(
            "⚠️ 메가박스 서버 과부하 감지 -> "
            f"현재 조회 묶음 즉시 중단 / "
            f"{cooldown:.0f}초 휴식 / "
            f"재시도 간격 {current_request_gap():.2f}s | "
            f"원인: {reason}"
        )


def note_clean_cycle():
    global _active_request_gap, _clean_cycle_streak, _overload_level

    changed = None
    with _adaptive_lock:
        _clean_cycle_streak += 1
        if (
            _clean_cycle_streak >= OVERLOAD_RECOVERY_CYCLES
            and _overload_level > 0
        ):
            _overload_level -= 1
            if _overload_level == 0:
                _active_request_gap = REQUEST_GAP
            else:
                _active_request_gap = OVERLOAD_GAP_STEPS[
                    _overload_level - 1
                ]
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
        if cycle_aborted():
            return False

        with _adaptive_lock:
            overload_until = _overload_until
            gap = _active_request_gap

        with _rate_lock:
            now = time.monotonic()
            target = max(_next_request_time, overload_until)

            if now < target:
                sleep_for = min(target - now, 0.25)
            else:
                _next_request_time = now + gap
                return True

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


def request_schedule(date, special=False):
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
        if cycle_aborted():
            return None, True, f"{label} {date} CYCLE ABORTED"

        if not wait_rate_slot():
            return None, True, f"{label} {date} CYCLE ABORTED"

        started = time.monotonic()
        session = get_session()

        try:
            response = session.post(
                SCHEDULE_API,
                data=params,
                headers=HEADERS,
                timeout=SCHEDULE_TIMEOUT,
            )
        except Exception as e:
            last_error = f"{label} {date} ERROR {repr(e)}"
            reset_thread_session()

            error_text = repr(e).lower()
            error_name = type(e).__name__.lower()
            is_timeout = (
                "timeout" in error_name
                or "timed out" in error_text
                or "curl: (28)" in error_text
            )

            # timeout은 메가박스 서버 과부하로 단정하지 않는다.
            if is_timeout:
                if attempt < 2:
                    print(
                        f"↻ {label} {date} timeout -> "
                        f"{CONNECTION_RETRY_DELAY:.1f}초 후 새 세션으로 1회 즉시 재시도"
                    )
                    time.sleep(CONNECTION_RETRY_DELAY)
                    continue
                return None, True, last_error

            if attempt < 2:
                time.sleep(CONNECTION_RETRY_DELAY)
                continue

            return None, True, last_error

        elapsed = time.monotonic() - started

        if response.status_code in TRUE_OVERLOAD_STATUSES:
            last_error = f"{label} {date} HTTP={response.status_code}"
            register_overload(f"HTTP {response.status_code}")
            reset_thread_session()
            return None, True, last_error

        # 500/502/504는 해당 날짜 요청의 일시 실패로만 처리.
        if response.status_code in TRANSIENT_HTTP_STATUSES:
            reset_thread_session()
            return None, True, (
                f"{label} {date} HTTP={response.status_code}"
            )

        if response.status_code != 200:
            return None, True, (
                f"{label} {date} HTTP={response.status_code}"
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
            return None, True, last_error

        try:
            data = response.json()
            rows = extract_movie_form_list(data)

            retry_text = f" ATTEMPT={attempt}" if attempt > 1 else ""
            return rows, False, (
                f"{label} {date} "
                f"HTTP=200 {elapsed:.2f}s "
                f"ROWS={len(rows)}{retry_text}"
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
            return None, True, last_error

    return None, True, f"{label} {date} UNKNOWN ERROR"


# ============================================================
# Row helpers / classification
# ============================================================

def all_text(row):
    values = []
    for key, value in row.items():
        if value is None:
            continue
        values.append(f"{key}={value}")
    return " ".join(values)


def is_stage(row):
    text = all_text(row)
    return "무대인사" in text or "舞台挨拶" in text


def is_megatalk(row):
    text = all_text(row)
    compact = re.sub(r"\s+", "", text)
    upper = text.upper()

    if "메가토크" in compact:
        return True
    if "관객과의대화" in compact:
        return True
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


def get_target_type(row, from_dolby=False):
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
    return html.unescape(str(value or "")).strip()


def get_movie(row):
    return clean_text(row.get("movieNm") or row.get("movNm") or "")


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

    date = clean_text(row.get("playDe") or "")
    if not date:
        date = now_kst().strftime("%Y%m%d")

    return (
        "https://www.megabox.co.kr/"
        "theater/time"
        f"?brchNo={BRANCH_NO}"
        f"&playDe={date}"
    )


def event_key(date, row, event_type):
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
        clean_text(row.get("movieNo") or ""),
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
    if event.get("sold_out_explicit"):
        return "SOLD_OUT"

    rest = event.get("rest_seat")
    if isinstance(rest, int):
        return "SOLD_OUT" if rest <= 0 else "OPEN"

    return "UNKNOWN"


def normalize_event(date, row, event_type):
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
    """공식 이벤트 페이지 HTML 안에서 코엑스 관련 선행 신호를 찾는다."""
    try:
        session = requests.Session(impersonate="chrome")
        response = session.get(
            EVENT_PAGE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9",
                "Referer": "https://www.megabox.co.kr/",
            },
            timeout=OFFICIAL_EVENT_TIMEOUT,
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

            snippet = context[:500]
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
        name = type(e).__name__
        print(
            f"⚠️ 공식 이벤트 페이지 일시 오류({name}) - "
            "이번 확인만 건너뛰고 다음 주기에 재시도"
        )
        return None


def send_official_signal(signal):
    event_type = signal.get("type") or "이벤트"
    display_type = (
        "DOLBY CINEMA"
        if event_type == "DOLBY"
        else event_type
    )
    url = signal.get("url", EVENT_PAGE_URL)

    lines = []
    if DISCORD_USER_ID:
        lines.append(f"<@{DISCORD_USER_ID}>")

    lines.extend([
        f"**🔎 {display_type} 공식 이벤트 신호가 감지됐습니다**",
        f"**[🎬 {BRANCH_NAME} · {display_type}]({url})**",
        f"🔎 {signal.get('snippet', '')}",
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
# Collect - 43 days / 2 workers / adaptive overload protection
# ============================================================

def collect_date(index, date):
    events = {}
    problem = False
    logs = []

    general_rows, general_problem, general_log = request_schedule(
        date,
        special=False,
    )
    logs.append(general_log)
    problem = problem or general_problem

    if cycle_aborted():
        return {
            "index": index,
            "date": date,
            "events": events,
            "problem": True,
            "logs": logs,
        }

    if general_rows is None:
        general_rows = []

    for row in general_rows:
        event_type = get_target_type(row, from_dolby=False)
        if event_type not in ("메가토크", "무대인사"):
            continue

        key = event_key(date, row, event_type)
        events[key] = normalize_event(date, row, event_type)

    dolby_rows, dolby_problem, dolby_log = request_schedule(
        date,
        special=True,
    )
    logs.append(dolby_log)
    problem = problem or dolby_problem

    if dolby_rows is None:
        dolby_rows = []

    for row in dolby_rows:
        event_type = get_target_type(row, from_dolby=True)
        if event_type is None:
            continue

        key = event_key(date, row, event_type)
        events[key] = normalize_event(date, row, event_type)

    return {
        "index": index,
        "date": date,
        "events": events,
        "problem": problem,
        "logs": logs,
    }


def collect_all_days(progress=False):
    all_events = {}
    failed_dates = []
    results = []
    dates = make_dates()

    reset_cycle_abort()
    reset_rate_clock()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(collect_date, index, date)
            for index, date in enumerate(dates, start=1)
        ]

        completed = 0
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print("DATE FUTURE ERROR:", repr(e))
            finally:
                completed += 1
                if progress and completed in {10, 20, 30, 40, DAYS}:
                    print(
                        f"⏳ 기준값 진행: {completed}/{DAYS} 날짜 처리 완료"
                    )

    results.sort(key=lambda item: item["index"])

    returned_dates = {item["date"] for item in results}
    for date in dates:
        if date not in returned_dates:
            failed_dates.append(date)

    error_examples = []
    for item in results:
        all_events.update(item["events"])

        if item["problem"]:
            failed_dates.append(item["date"])
            if len(error_examples) < 3:
                error_examples.append(
                    f"{item['date']}: " + " | ".join(item["logs"])
                )

    if error_examples:
        print(
            "⚠️ API 오류 예시(최대 3건): "
            + " || ".join(error_examples)
        )

    failed_dates = sorted(set(failed_dates))
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
    # 처음 발견했을 때 이미 매진인 회차는 알리지 않고 seen에만 등록.
    for key, event in events.items():
        if key not in seen and booking_status(event) == "SOLD_OUT":
            seen.add(key)

    new_events = [
        (key, event)
        for key, event in events.items()
        if key not in seen
        and booking_status(event) != "SOLD_OUT"
    ]

    if not new_events:
        return 0, set()

    groups = {}
    for key, event in new_events:
        group_key = (event["date"], event["type"])
        groups.setdefault(group_key, []).append((key, event))

    sent_count = 0
    sent_keys = set()

    for (date, event_type), items in sorted(groups.items()):
        display_type = (
            "DOLBY CINEMA"
            if event_type == "DOLBY"
            else event_type
        )

        lines = []
        if DISCORD_USER_ID:
            lines.append(f"<@{DISCORD_USER_ID}>")

        lines.extend([
            f"**🔎 {display_type}가 감지됐습니다**",
            f"**🎬 {BRANCH_NAME} · {display_type}**",
            f"**📅 {pretty_date(date)}**",
        ])

        items.sort(
            key=lambda x: (
                x[1]["start"],
                x[1]["movie"],
            )
        )

        for key, event in items:
            start = event.get("start", "")
            end = event.get("end", "")
            time_text = f"{start}–{end}" if end else start
            movie = event.get("movie") or "영화명 확인 필요"
            screen = event.get("screen") or "상영관 정보 없음"
            link = event.get("link") or (
                f"https://www.megabox.co.kr/theater/time?brchNo={BRANCH_NO}"
            )
            lines.append(
                f"**[🎟 {time_text} · {movie} · {screen}]({link})**"
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


def process_booking_states(events, show_state):
    """
    예매 상태는 내부 중복방지/상태 보존용으로만 기록한다.
    매진, 잔여 0, 매진 -> OPEN 변화는 사용자 로그/Discord에 알리지 않는다.
    """
    for key, event in events.items():
        current = booking_status(event)
        previous_record = show_state.get(key) or {}
        previous = previous_record.get("status")

        if current == "UNKNOWN":
            if previous is None:
                show_state[key] = state_record(event, "UNKNOWN")
            else:
                show_state[key] = state_record(event, previous)
            continue

        show_state[key] = state_record(event, current)

    return 0, 0


# ============================================================
# Staggered 43-day scheduler
# GENERAL / DOLBY use the SAME interval for each date.
# ============================================================

def interval_for_offset(offset):
    if offset <= 1:
        return INTERVAL_0_1
    if offset <= 4:
        return INTERVAL_2_4
    if offset <= 14:
        return INTERVAL_5_14
    if offset <= 30:
        return INTERVAL_15_30
    return INTERVAL_31_42


def effective_interval(date, offset, show_state):
    # 매진 여부와 무관하게 날짜 구간별 기본 주기만 사용.
    return interval_for_offset(offset)


def error_retry_interval(offset):
    # 실패한 날짜만 짧게 재확인. 전체 감시는 멈추지 않는다.
    if offset <= 1:
        return 10.0
    if offset <= 14:
        return 20.0
    return 60.0


def build_due_schedule(show_state):
    """43개 날짜를 각 구간 interval 안에서 균등 분산한다."""
    now = time.monotonic()
    dates = make_dates()
    groups = {}

    for offset, date in enumerate(dates):
        interval = effective_interval(date, offset, show_state)
        groups.setdefault(interval, []).append((offset, date))

    next_due = {}
    for interval, items in groups.items():
        count = max(1, len(items))
        for pos, (_offset, date) in enumerate(items):
            next_due[date] = now + (interval * pos / count)

    return next_due


def collect_due_dates(dates):
    """현재 due인 날짜만 GENERAL + DOLBY를 같이 확인한다."""
    if not dates:
        return {}, []

    all_events = {}
    failed_dates = []
    results = []

    reset_cycle_abort()
    reset_rate_clock()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        full_dates = make_dates()
        index_map = {d: i + 1 for i, d in enumerate(full_dates)}

        for date in dates:
            futures.append(
                executor.submit(
                    collect_date,
                    index_map.get(date, 0),
                    date,
                )
            )

        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print("DATE FUTURE ERROR:", repr(e))

    error_examples = []
    for item in results:
        # GENERAL/DOLBY 한쪽만 성공해도 성공한 쪽 이벤트는 감지에 사용.
        all_events.update(item.get("events", {}))

        if item.get("problem"):
            failed_dates.append(item.get("date", ""))
            if len(error_examples) < 2:
                error_examples.append(
                    f"{item.get('date', '')}: "
                    + " | ".join(item.get("logs", []))
                )

    returned_dates = {item.get("date") for item in results}
    for date in dates:
        if date not in returned_dates:
            failed_dates.append(date)

    failed_dates = sorted(set(d for d in failed_dates if d))

    if error_examples:
        print(
            "⚠️ API 오류 예시(최대 2건): "
            + " || ".join(error_examples)
        )

    return all_events, failed_dates


def state_count_cache(show_state):
    """heartbeat 표시용 시작 count cache."""
    valid_dates = set(make_dates())
    cache = {}

    for record in show_state.values():
        if not isinstance(record, dict):
            continue

        date = record.get("date")
        kind = record.get("type")
        if date not in valid_dates or kind not in {"메가토크", "무대인사", "DOLBY"}:
            continue

        bucket = cache.setdefault(
            date,
            {"메가토크": 0, "무대인사": 0, "DOLBY": 0},
        )
        bucket[kind] += 1

    return cache


def total_counts_from_cache(cache):
    total = {"메가토크": 0, "무대인사": 0, "DOLBY": 0}
    for counts in cache.values():
        for key in total:
            total[key] += counts.get(key, 0)
    return total


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

    print("BRANCH:", BRANCH_NAME)
    print("BRANCH NO:", BRANCH_NO)
    print("TARGET: 메가토크(GV 포함) / 무대인사 / DOLBY CINEMA")
    print("DATE RANGE: TODAY ~ +42 DAYS (43 DAYS TOTAL)")
    print(
        "SCAN MODE: "
        "43 DAYS STAGGERED / GENERAL+DOLBY SAME INTERVAL / 2 WORKERS"
    )
    print(
        "REQUEST START GAP:",
        f"{REQUEST_GAP:.2f}s START / overload auto-backoff",
    )
    print(
        "DATE INTERVALS: "
        "0~1일 20s / 2~4일 90s / 5~14일 30s / "
        "15~30일 60s / 31~42일 300s"
    )
    print("GENERAL / DOLBY: 같은 날짜는 같은 감시 주기")
    print(
        "00/30 FAST SCAN: +4~+21일 18일 / 2 workers / 0.17s gap"
    )
    print(
        "EARLY SIGNAL: 공식 이벤트 페이지 / "
        "메가토크(GV·관객과의 대화 포함) / 무대인사"
    )
    print("SOLD OUT / REOPEN: 사용자 알림 없음 / 내부 상태만 저장")
    print(
        f"LOG MODE: 기준값 10/20/30/40/{DAYS} 진행 + "
        "정상 감시는 10분 요약 / timeout은 날짜별 재시도 / "
        "실제 과부하만 자동완화"
    )
    print("RUN SECONDS:", RUN_SECONDS)
    print("KST NOW:", now_kst().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 72)

    try:
        session = requests.Session(impersonate="chrome")
        r = session.get(
            "https://www.megabox.co.kr/",
            headers=HEADERS,
            timeout=MAIN_PAGE_TIMEOUT,
        )
        print("MEGABOX PAGE STATUS:", r.status_code)
    except Exception as e:
        print("MEGABOX PAGE CHECK WARNING:", repr(e))
        print("MAIN PAGE CHECK FAILED - CONTINUE MONITORING")

    # --------------------------------------------------------
    # 최초/업그레이드 기준값
    # --------------------------------------------------------
    if not baseline_done():
        print()
        print("=" * 72)
        print("INITIAL 43-DAY BASELINE")
        print("=" * 72)
        print(
            "현재 43일 전체 메가토크(GV 포함) / 무대인사 / DOLBY와 "
            "공식 이벤트 선행 신호를 알림 없이 기준값으로 등록합니다."
        )

        events, failed_dates = collect_all_days(progress=True)

        if failed_dates:
            print("BASELINE FAILED DATES:", ", ".join(failed_dates))
            print("불완전한 기준값은 저장하지 않습니다.")
            return

        seen = set(events.keys())

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

        print("BASELINE EVENT COUNT:", len(seen))
        print("BASELINE OFFICIAL SIGNAL COUNT:", len(official_signals))
        print_counts(events)

        save_seen(seen)
        save_status(status)
        mark_baseline_done()

        print("BASELINE COMPLETE")
        print("이번 기준값 등록에서는 Discord 알림을 보내지 않았습니다.")
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
    next_due = build_due_schedule(show_state)
    offsets = {date: i for i, date in enumerate(make_dates())}
    last_official_event_check = 0.0
    next_safety_scan_at = next_halfhour_boundary(now_kst())

    date_count_cache = state_count_cache(show_state)
    latest_counts = total_counts_from_cache(date_count_cache)

    heartbeat_started = monitor_started
    heartbeat_date_checks = 0
    heartbeat_new = 0
    heartbeat_cancel = 0
    heartbeat_soldout = 0
    heartbeat_official = 0
    heartbeat_failed_dates = 0

    print(
        "✅ 분산 감시 시작 | GENERAL/DOLBY 동일 주기 | "
        "오늘·내일 20초 / +2~+4일 90초 / +5~+14일 30초 / "
        "+15~+30일 60초 / +31~+42일 5분"
    )

    while True:
        now_mono = time.monotonic()
        if now_mono - monitor_started >= RUN_SECONDS:
            print("MONITOR TIME FINISHED")
            break

        # Due dates are sorted so old overdue dates are handled first.
        due_dates = [
            date
            for date, due in sorted(next_due.items(), key=lambda x: x[1])
            if due <= now_mono
        ][:MAX_DUE_DATES_PER_BATCH]

        if due_dates:
            overload_before = overload_event_count()
            events, failed_dates = collect_due_dates(due_dates)
            batch_overloads = overload_event_count() - overload_before

            valid_dates = set(due_dates) - set(failed_dates)
            valid_events = dict(events)

            if batch_overloads > 0 and valid_events:
                print(
                    "🛡️ 실제 과부하 신호가 있었지만 성공 응답에서 확보한 "
                    "이벤트는 감지에 사용하고, 실패 소스의 상태는 유지합니다."
                )

            if valid_events or valid_dates:
                new_count, _sent_keys = send_new_events(valid_events, seen)
                cancel_count, sold_out_count = process_booking_states(
                    valid_events,
                    show_state,
                )

                # 성공한 날짜만 heartbeat count cache를 교체.
                for date in valid_dates:
                    date_events = {
                        k: e
                        for k, e in valid_events.items()
                        if e.get("date") == date
                    }
                    date_count_cache[date] = count_events(date_events)

                latest_counts = total_counts_from_cache(date_count_cache)
                save_seen(seen)
                save_status(status)
            else:
                new_count = 0
                cancel_count = 0
                sold_out_count = 0

            heartbeat_date_checks += len(due_dates)
            heartbeat_new += new_count
            heartbeat_cancel += cancel_count
            heartbeat_soldout += sold_out_count
            heartbeat_failed_dates += len(failed_dates)

            reschedule_base = time.monotonic()
            for date in due_dates:
                offset = offsets.get(date, DAYS - 1)
                interval = effective_interval(date, offset, show_state)

                if date in failed_dates:
                    interval = min(
                        interval,
                        error_retry_interval(offset),
                    )

                next_due[date] = reschedule_base + interval

            if not failed_dates and batch_overloads == 0:
                note_clean_cycle()

        # ----------------------------------------------------
        # 매시 00분 / 30분 빠른 전체점검
        # ----------------------------------------------------
        now_wall = now_kst()
        if now_wall >= next_safety_scan_at:
            safety_slot = next_safety_scan_at
            safety_dates = make_safety_scan_dates()

            print(
                f"🔎 {safety_slot.strftime('%H:%M')} 빠른 전체점검 시작 | "
                f"+4~+21일 {len(safety_dates)}일 | 2 workers / 0.17s gap"
            )

            safety_started = time.monotonic()
            overload_before = overload_event_count()
            safety_events, safety_failed = collect_due_dates(safety_dates)
            safety_overloads = overload_event_count() - overload_before

            safety_valid_dates = set(safety_dates) - set(safety_failed)
            safety_valid_events = dict(safety_events)

            if safety_overloads > 0 and safety_valid_events:
                print(
                    "🛡️ 00/30 빠른점검 중 실제 과부하 신호가 있었지만 "
                    "성공 응답에서 확보한 이벤트는 감지에 사용합니다."
                )

            if safety_valid_events or safety_valid_dates:
                safety_new, _safety_sent_keys = send_new_events(
                    safety_valid_events,
                    seen,
                )
                safety_cancel, safety_soldout = process_booking_states(
                    safety_valid_events,
                    show_state,
                )

                for date in safety_valid_dates:
                    date_events = {
                        k: e
                        for k, e in safety_valid_events.items()
                        if e.get("date") == date
                    }
                    date_count_cache[date] = count_events(date_events)

                latest_counts = total_counts_from_cache(date_count_cache)
                save_seen(seen)
                save_status(status)
            else:
                safety_new = 0
                safety_cancel = 0
                safety_soldout = 0

            heartbeat_date_checks += len(safety_dates)
            heartbeat_new += safety_new
            heartbeat_cancel += safety_cancel
            heartbeat_soldout += safety_soldout
            heartbeat_failed_dates += len(safety_failed)

            # 방금 빠른점검한 날짜는 정상 주기만큼 다음 조회를 미룬다.
            reschedule_base = time.monotonic()
            for date in safety_dates:
                offset = offsets.get(date)
                if offset is None:
                    continue

                interval = effective_interval(date, offset, show_state)
                if date in safety_failed:
                    interval = min(
                        interval,
                        error_retry_interval(offset),
                    )
                next_due[date] = reschedule_base + interval

            safety_elapsed = time.monotonic() - safety_started
            safety_error_count = len(safety_failed) + safety_overloads
            safety_ok_count = len(safety_valid_dates)
            safety_icon = "⚡" if safety_error_count == 0 else "⚠️"

            print(
                f"{safety_icon} {safety_slot.strftime('%H:%M')} 00/30 동시스캔 완료 | "
                f"+{SAFETY_SCAN_START_OFFSET}~+{SAFETY_SCAN_END_OFFSET}일 | "
                f"성공 {safety_ok_count}/{len(safety_dates)} | "
                f"{safety_elapsed:.2f}초 | "
                f"Discord 알림 {safety_new} | "
                f"오류 {safety_error_count}"
            )

            if safety_error_count == 0:
                note_clean_cycle()

            while next_safety_scan_at <= now_kst():
                next_safety_scan_at += timedelta(
                    minutes=SAFETY_SCAN_EVERY_MINUTES
                )

        # ----------------------------------------------------
        # 공식 이벤트 페이지 선행 신호
        # ----------------------------------------------------
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
            heartbeat_official += official_count

            if official_count:
                save_status(status)

            last_official_event_check = now_mono

        # ----------------------------------------------------
        # 💚 10-minute compact heartbeat
        # ----------------------------------------------------
        now_mono = time.monotonic()
        if now_mono - heartbeat_started >= HEARTBEAT_INTERVAL:
            mins = int((now_mono - heartbeat_started) // 60)

            health = (
                "💚 정상 감시중"
                if heartbeat_failed_dates == 0
                else "⚠️ 감시중(일부 API 오류)"
            )

            print(
                f"{health} | 최근 {mins}분 날짜조회 {heartbeat_date_checks}건 | "
                f"메가토크 {latest_counts['메가토크']} | "
                f"무대인사 {latest_counts['무대인사']} | "
                f"DOLBY {latest_counts['DOLBY']} | "
                f"새 일정 {heartbeat_new} | "
                f"공식선행 {heartbeat_official} | "
                f"API실패 {heartbeat_failed_dates}건 | "
                f"현재 요청간격 {current_request_gap():.2f}s"
            )

            heartbeat_started = now_mono
            heartbeat_date_checks = 0
            heartbeat_new = 0
            heartbeat_cancel = 0
            heartbeat_soldout = 0
            heartbeat_official = 0
            heartbeat_failed_dates = 0

        # 다음 due까지 짧게 sleep.
        next_date_due = (
            min(next_due.values())
            if next_due
            else now_mono + 1.0
        )
        next_official_due = (
            last_official_event_check
            + OFFICIAL_EVENT_CHECK_INTERVAL
        )
        next_heartbeat_due = heartbeat_started + HEARTBEAT_INTERVAL
        seconds_to_safety = max(
            0.0,
            (next_safety_scan_at - now_kst()).total_seconds(),
        )
        next_safety_due = time.monotonic() + seconds_to_safety

        next_wake = min(
            next_date_due,
            next_official_due,
            next_heartbeat_due,
            next_safety_due,
        )

        sleep_for = max(
            0.05,
            min(1.0, next_wake - time.monotonic()),
        )
        time.sleep(sleep_for)

    save_seen(seen)
    save_status(status)

    print("FINAL SEEN STATE:", len(seen))
    print("DONE")


if __name__ == "__main__":
    main()
