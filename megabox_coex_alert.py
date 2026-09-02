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
#
# 대상:
# - 메가토크 (GV / 관객과의 대화 포함)
# - 무대인사
# - DOLBY CINEMA
#
# 최종 감시 방식
# - 오늘~+42일 = 43일
# - GENERAL / DOLBY 같은 날짜 구간 주기로 분산 감시
# - 0~1일 20초 / 2~4일 90초 / 5~14일 30초
# - 15~30일 60초 / 31~42일 300초
# - 매시 00분/30분: +4~+21일 18일 빠른 전체점검
# - 2 workers 고정
# - 모든 API 요청 시작 간격 기본 0.17초
# - timeout: 즉시 재시도하지 않음. 해당 날짜/소스만 실패 처리
# - 403/429/503: 과부하 보호 0.30/0.40/0.50초 + 60/120/300초 휴식
# - 정상 요약: 10분마다 1줄
# ============================================================

BRANCH_NO = "1351"
BRANCH_NAME = "메가박스 코엑스"

DAYS = 43
WORKERS = 2

REQUEST_GAP = 0.17
OVERLOAD_GAP_STEPS = (0.30, 0.40, 0.50)
OVERLOAD_COOLDOWN_STEPS = (60.0, 120.0, 300.0)
OVERLOAD_RECOVERY_CYCLES = 10

SCHEDULE_TIMEOUT = 7.0
MAIN_PAGE_TIMEOUT = 4.0
OFFICIAL_EVENT_TIMEOUT = 4.0
CONNECTION_RETRY_DELAY = 0.5

TRUE_OVERLOAD_STATUSES = {403, 429, 503}
TRANSIENT_HTTP_STATUSES = {500, 502, 504}

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))

KST = ZoneInfo("Asia/Seoul")

SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

EVENT_PAGE_URL = "https://www.megabox.co.kr/event"

STATE_FILE = "seen_megabox_coex.json"
STATUS_FILE = "status_megabox_coex.json"
BASELINE_FILE = "baseline_megabox_coex.done"

OFFICIAL_EVENT_CHECK_INTERVAL = 120.0
HEARTBEAT_INTERVAL = 600.0

INTERVAL_0_1 = 20.0
INTERVAL_2_4 = 90.0
INTERVAL_5_14 = 30.0
INTERVAL_15_30 = 60.0
INTERVAL_31_42 = 300.0

MAX_DUE_DATES_PER_BATCH = 2

SAFETY_SCAN_START_OFFSET = 4
SAFETY_SCAN_END_OFFSET = 21

# 기존 43일 분산감시 기준값과 호환
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
_overload_level = 0
_clean_cycle_streak = 0

_cycle_abort_event = threading.Event()


# ============================================================
# Time
# ============================================================

def now_kst():
    return datetime.now(KST)


def make_dates():
    today = now_kst().date()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    ]


def make_safety_scan_dates():
    today = now_kst().date()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(
            SAFETY_SCAN_START_OFFSET,
            SAFETY_SCAN_END_OFFSET + 1,
        )
    ]


def pretty_date(date):
    dt = datetime.strptime(date, "%Y%m%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return (
        f"{dt.year}.{dt.month}.{dt.day}"
        f"({weekdays[dt.weekday()]})"
    )


def next_halfhour_boundary(dt):
    base = dt.replace(second=0, microsecond=0)
    if dt.minute < 30:
        return base.replace(minute=30)
    return base.replace(minute=0) + timedelta(hours=1)


def date_offset(date):
    target = datetime.strptime(date, "%Y%m%d").date()
    return max(0, (target - now_kst().date()).days)


def interval_for_date(date):
    offset = date_offset(date)

    if offset <= 1:
        return INTERVAL_0_1
    if offset <= 4:
        return INTERVAL_2_4
    if offset <= 14:
        return INTERVAL_5_14
    if offset <= 30:
        return INTERVAL_15_30
    return INTERVAL_31_42


def stagger_schedule(dates, start_at=None):
    if start_at is None:
        start_at = time.monotonic()

    groups = {}
    for date in dates:
        interval = interval_for_date(date)
        groups.setdefault(interval, []).append(date)

    next_due = {}

    for interval, group_dates in groups.items():
        spacing = interval / max(1, len(group_dates))

        for index, date in enumerate(group_dates):
            next_due[date] = start_at + index * spacing

    return next_due


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
        official = data.get("official_events")

        return {
            "shows": shows if isinstance(shows, dict) else {},
            "official_events": (
                official if isinstance(official, dict) else {}
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
            return f.read().strip() == BASELINE_SCHEMA

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


def current_request_gap():
    with _adaptive_lock:
        return _active_request_gap


def reset_rate_clock():
    global _next_request_time

    with _rate_lock:
        _next_request_time = time.monotonic()


def reset_cycle_abort():
    _cycle_abort_event.clear()


def cycle_aborted():
    return _cycle_abort_event.is_set()


def register_overload(reason):
    global _active_request_gap
    global _overload_until
    global _overload_level
    global _clean_cycle_streak

    now = time.monotonic()
    should_log = False
    cooldown = 0.0

    _cycle_abort_event.set()

    with _adaptive_lock:
        _clean_cycle_streak = 0

        # 동일 과부하 파동 중복 승격 방지
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
            "⚠️ 메가박스 서버 과부하 감지 | "
            f"{cooldown:.0f}초 휴식 | "
            f"요청간격 {current_request_gap():.2f}s | "
            f"{reason}"
        )


def note_clean_batch():
    global _active_request_gap
    global _overload_level
    global _clean_cycle_streak

    changed = None

    with _adaptive_lock:
        if _overload_level <= 0:
            return

        _clean_cycle_streak += 1

        if _clean_cycle_streak >= OVERLOAD_RECOVERY_CYCLES:
            _overload_level -= 1
            _clean_cycle_streak = 0

            if _overload_level == 0:
                _active_request_gap = REQUEST_GAP
            else:
                _active_request_gap = OVERLOAD_GAP_STEPS[
                    _overload_level - 1
                ]

            changed = _active_request_gap

    if changed is not None:
        print(
            "✅ 과부하 없이 10묶음 완료 | "
            f"요청간격 {changed:.2f}s로 복구"
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

            if now >= target:
                _next_request_time = now + gap
                return True

            sleep_for = min(target - now, 0.25)

        time.sleep(max(0.01, sleep_for))


def extract_movie_form_list(data):
    if not isinstance(data, dict):
        return []

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

    # timeout은 즉시 재시도하지 않는다.
    # timeout 외 실제 연결 오류만 새 세션으로 1회 재시도.
    attempts = 2

    for attempt in range(1, attempts + 1):
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
            error_text = repr(e)
            lower = error_text.lower()
            name = type(e).__name__.lower()

            reset_thread_session()

            is_timeout = (
                "timeout" in name
                or "timed out" in lower
                or "curl: (28)" in lower
            )

            # 핵심 수정:
            # timeout이면 0.5초 뒤 즉시 재시도하지 않고 여기서 종료.
            if is_timeout:
                return (
                    None,
                    True,
                    f"{label} {date} TIMEOUT",
                )

            if attempt < attempts:
                time.sleep(CONNECTION_RETRY_DELAY)
                continue

            return (
                None,
                True,
                f"{label} {date} CONNECTION ERROR",
            )

        elapsed = time.monotonic() - started

        if response.status_code in TRUE_OVERLOAD_STATUSES:
            register_overload(
                f"HTTP {response.status_code}"
            )
            reset_thread_session()

            return (
                None,
                True,
                f"{label} {date} HTTP={response.status_code}",
            )

        if response.status_code in TRANSIENT_HTTP_STATUSES:
            reset_thread_session()

            return (
                None,
                True,
                f"{label} {date} HTTP={response.status_code}",
            )

        if response.status_code != 200:
            return (
                None,
                True,
                f"{label} {date} HTTP={response.status_code}",
            )

        preview = (
            response.text[:160]
            .replace("\n", " ")
            .replace("\r", " ")
        )

        if "Workload is so high" in preview:
            register_overload("Workload is so high")
            reset_thread_session()

            return (
                None,
                True,
                f"{label} {date} SERVER OVERLOAD",
            )

        try:
            data = response.json()
            rows = extract_movie_form_list(data)

            return (
                rows,
                False,
                (
                    f"{label} {date} "
                    f"HTTP=200 {elapsed:.2f}s "
                    f"ROWS={len(rows)}"
                ),
            )

        except Exception:
            register_overload("HTTP 200 비정상 JSON")
            reset_thread_session()

            return (
                None,
                True,
                f"{label} {date} JSON ERROR",
            )

    return None, True, f"{label} {date} UNKNOWN ERROR"


# ============================================================
# Row helpers / classification
# ============================================================

def clean_text(value):
    return html.unescape(str(value or "")).strip()


def all_text(row):
    return " ".join(
        f"{key}={value}"
        for key, value in row.items()
        if value is not None
    )


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

    return (
        "메가토크" in compact
        or "관객과의대화" in compact
        or bool(
            re.search(
                r"(?<![A-Z0-9])GV(?![A-Z0-9])",
                upper,
            )
        )
    )


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


def to_int(value):
    try:
        if value is None or value == "":
            return None

        return int(
            str(value)
            .replace(",", "")
            .strip()
        )

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
        or now_kst().strftime("%Y%m%d")
    )

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


def normalize_event(date, row, event_type):
    row_text = all_text(row)
    upper = row_text.upper()

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
        "sold_out_explicit": (
            "매진" in row_text
            or "SOLD_OUT" in upper
            or "SOLDOUT" in upper
        ),
    }


def booking_status(event):
    if event.get("sold_out_explicit"):
        return "SOLD_OUT"

    rest = event.get("rest_seat")

    if isinstance(rest, int):
        return (
            "SOLD_OUT"
            if rest <= 0
            else "OPEN"
        )

    return "UNKNOWN"


# ============================================================
# Official event page - best effort
# ============================================================

def compact_ws(text):
    return re.sub(
        r"\s+",
        " ",
        clean_text(text),
    ).strip()


def strip_tags(text):
    text = re.sub(
        r"(?is)<(script|style).*?>.*?</\1>",
        " ",
        text,
    )

    text = re.sub(
        r"(?s)<[^>]+>",
        " ",
        text,
    )

    return compact_ws(
        html.unescape(text)
    )


def classify_event_text(text):
    compact = re.sub(r"\s+", "", text)
    upper = text.upper()

    if (
        "무대인사" in compact
        or "舞台挨拶" in text
    ):
        return "무대인사"

    if (
        "메가토크" in compact
        or "관객과의대화" in compact
        or re.search(
            r"(?<![A-Z0-9])GV(?![A-Z0-9])",
            upper,
        )
    ):
        return "메가토크"

    return None


def fetch_official_event_signals():
    try:
        session = requests.Session(
            impersonate="chrome"
        )

        response = session.get(
            EVENT_PAGE_URL,
            headers={
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9",
                "Referer": "https://www.megabox.co.kr/",
            },
            timeout=OFFICIAL_EVENT_TIMEOUT,
        )

        if response.status_code != 200:
            return None

        visible = strip_tags(response.text)
        signals = {}

        keyword_re = re.compile(
            (
                r"무대인사|舞台挨拶|메가토크|"
                r"관객\s*과의\s*대화|"
                r"(?<![A-Z0-9])GV(?![A-Z0-9])"
            ),
            flags=re.I,
        )

        for match in keyword_re.finditer(visible):
            start = max(0, match.start() - 350)
            end = min(
                len(visible),
                match.end() + 350,
            )

            context = compact_ws(
                visible[start:end]
            )

            if "코엑스" not in context:
                continue

            event_type = classify_event_text(
                context
            )

            if event_type is None:
                continue

            snippet = context[:500]

            signature = hashlib.sha1(
                (
                    event_type
                    + "|"
                    + snippet
                ).encode("utf-8")
            ).hexdigest()

            signals[signature] = {
                "type": event_type,
                "snippet": snippet,
                "url": EVENT_PAGE_URL,
                "detected_at_kst": (
                    now_kst().isoformat(
                        timespec="seconds"
                    )
                ),
            }

        return signals

    except Exception:
        # 보조 소스이므로 조용히 다음 주기로 넘김
        return None


def send_official_signal(signal):
    event_type = signal.get("type") or "이벤트"
    url = signal.get("url", EVENT_PAGE_URL)

    lines = []

    if DISCORD_USER_ID:
        lines.append(
            f"<@{DISCORD_USER_ID}>"
        )

    lines.extend([
        f"**🔎 {event_type} 공식 이벤트 신호가 감지됐습니다**",
        f"**[🎬 {BRANCH_NAME} · {event_type}]({url})**",
        f"🔎 {signal.get('snippet', '')}",
    ])

    return send_discord(
        "\n".join(lines)
    )


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
# Collect
# ============================================================

def collect_date(index, date):
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

    if cycle_aborted():
        return {
            "index": index,
            "date": date,
            "events": events,
            "problem": True,
            "logs": logs,
        }

    for row in general_rows or []:
        event_type = get_target_type(
            row,
            from_dolby=False,
        )

        if event_type not in {
            "메가토크",
            "무대인사",
        }:
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

    for row in dolby_rows or []:
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


def collect_dates(dates, progress=False):
    all_events = {}
    failed_dates = []
    error_examples = []

    reset_cycle_abort()
    reset_rate_clock()

    results = []

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
            completed += 1

            try:
                item = future.result()
                results.append(item)

            except Exception as e:
                if len(error_examples) < 2:
                    error_examples.append(
                        f"WORKER ERROR {type(e).__name__}"
                    )

            if (
                progress
                and completed in {
                    10,
                    20,
                    30,
                    40,
                    len(dates),
                }
            ):
                print(
                    f"⏳ 기준값 진행: "
                    f"{completed}/{len(dates)} 날짜 처리 완료"
                )

    for item in results:
        all_events.update(
            item["events"]
        )

        if item["problem"]:
            failed_dates.append(
                item["date"]
            )

            if len(error_examples) < 2:
                concise_logs = []

                for log_text in item["logs"]:
                    if "TIMEOUT" in log_text:
                        concise_logs.append(
                            log_text
                        )
                    elif (
                        "HTTP=" in log_text
                        and "HTTP=200" not in log_text
                    ):
                        concise_logs.append(
                            log_text
                        )

                if concise_logs:
                    error_examples.append(
                        " | ".join(
                            concise_logs
                        )
                    )

    failed_dates = sorted(
        set(failed_dates)
    )

    return (
        all_events,
        failed_dates,
        error_examples,
    )


def collect_all_days(progress=False):
    return collect_dates(
        make_dates(),
        progress=progress,
    )


# ============================================================
# Event processing
# ============================================================

def state_record(event):
    return {
        "status": booking_status(event),
        "date": event.get("date", ""),
        "type": event.get("type", ""),
        "movie": event.get("movie", ""),
        "start": event.get("start", ""),
        "end": event.get("end", ""),
        "screen": event.get("screen", ""),
        "schedule_no": event.get("schedule_no", ""),
        "rest_seat": event.get("rest_seat"),
        "total_seat": event.get("total_seat"),
        "sold_out_explicit": bool(
            event.get("sold_out_explicit")
        ),
        "updated_at_kst": (
            now_kst().isoformat(
                timespec="seconds"
            )
        ),
    }


def send_new_events(events, seen):
    # 처음 발견 시 매진인 회차는 알리지 않고 seen만 등록
    for key, event in events.items():
        if (
            key not in seen
            and booking_status(event) == "SOLD_OUT"
        ):
            seen.add(key)

    new_events = [
        (key, event)
        for key, event in events.items()
        if (
            key not in seen
            and booking_status(event) != "SOLD_OUT"
        )
    ]

    if not new_events:
        return 0

    groups = {}

    for key, event in new_events:
        group_key = (
            event["date"],
            event["type"],
        )

        groups.setdefault(
            group_key,
            [],
        ).append(
            (key, event)
        )

    sent_count = 0

    for (
        date,
        event_type,
    ), items in sorted(groups.items()):
        display_type = (
            "DOLBY CINEMA"
            if event_type == "DOLBY"
            else event_type
        )

        lines = []

        if DISCORD_USER_ID:
            lines.append(
                f"<@{DISCORD_USER_ID}>"
            )

        lines.extend([
            f"**🔎 {display_type}가 감지됐습니다**",
            f"**🎬 {BRANCH_NAME} · {display_type}**",
            f"**📅 {pretty_date(date)}**",
        ])

        items.sort(
            key=lambda item: (
                item[1].get("start", ""),
                item[1].get("movie", ""),
            )
        )

        for key, event in items:
            start = event.get("start", "")
            end = event.get("end", "")
            when = (
                f"{start}–{end}"
                if end
                else start
            )

            movie = (
                event.get("movie")
                or "영화명 확인 필요"
            )

            screen = (
                event.get("screen")
                or "상영관 정보 없음"
            )

            link = (
                event.get("link")
                or (
                    "https://www.megabox.co.kr/"
                    f"theater/time?brchNo={BRANCH_NO}"
                )
            )

            lines.append(
                f"**[🎟 {when} · {movie} · {screen}]({link})**"
            )

        if send_discord(
            "\n".join(lines)
        ):
            for key, _ in items:
                seen.add(key)
                sent_count += 1

    return sent_count


def merge_date_state(
    date,
    events,
    show_state,
):
    # 실패하지 않은 날짜만 기존 해당 날짜 상태를 새 값으로 교체
    to_delete = [
        key
        for key, record in show_state.items()
        if (
            isinstance(record, dict)
            and record.get("date") == date
        )
    ]

    for key in to_delete:
        del show_state[key]

    for key, event in events.items():
        show_state[key] = state_record(event)


def count_from_state(show_state):
    counts = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    valid_dates = set(
        make_dates()
    )

    for record in show_state.values():
        if not isinstance(record, dict):
            continue

        if record.get("date") not in valid_dates:
            continue

        event_type = record.get("type")

        if event_type in counts:
            counts[event_type] += 1

    return counts


# ============================================================
# Baseline
# ============================================================

def initialize_baseline():
    print()
    print("=" * 72)
    print("INITIAL 43-DAY BASELINE")
    print("=" * 72)

    events, failed_dates, error_examples = (
        collect_all_days(
            progress=True
        )
    )

    if failed_dates:
        print(
            f"⚠️ 기준값 API 실패 {len(failed_dates)}개 날짜"
        )

        if error_examples:
            print(
                "⚠️ 오류 예시: "
                + " || ".join(
                    error_examples[:2]
                )
            )

        print(
            "불완전한 기준값은 저장하지 않습니다."
        )
        return False

    seen = set(
        events.keys()
    )

    official_signals = (
        fetch_official_event_signals()
        or {}
    )

    status = {
        "shows": {
            key: state_record(event)
            for key, event in events.items()
        },
        "official_events": official_signals,
    }

    save_seen(seen)
    save_status(status)
    mark_baseline_done()

    counts = count_from_state(
        status["shows"]
    )

    print(
        "✅ 기준값 등록 완료 | "
        f"메가토크 {counts['메가토크']} | "
        f"무대인사 {counts['무대인사']} | "
        f"DOLBY {counts['DOLBY']}"
    )

    return True


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 72)
    print("MEGABOX COEX MONITOR")
    print("=" * 72)

    print("BRANCH:", BRANCH_NAME)
    print("BRANCH NO:", BRANCH_NO)

    print(
        "TARGET: "
        "메가토크(GV 포함) / 무대인사 / DOLBY CINEMA"
    )

    print(
        "DATE RANGE: "
        "TODAY ~ +42 DAYS (43 DAYS TOTAL)"
    )

    print(
        "DATE INTERVALS: "
        "0~1일 20s / 2~4일 90s / "
        "5~14일 30s / 15~30일 60s / "
        "31~42일 300s"
    )

    print(
        "00/30 FAST SCAN: "
        "+4~+21일 18일 / 2 workers / 0.17s"
    )

    print(
        "TIMEOUT: 즉시 재시도 없음 / "
        "다음 해당 날짜 주기에 다시 확인"
    )

    print(
        "OVERLOAD: 403/429/503 -> "
        "0.30/0.40/0.50s + 60/120/300s"
    )

    print(
        "LOG MODE: 정상 요약 10분마다 1줄"
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

        response = session.get(
            "https://www.megabox.co.kr/",
            headers=HEADERS,
            timeout=MAIN_PAGE_TIMEOUT,
        )

        print(
            "MEGABOX PAGE STATUS:",
            response.status_code,
        )

    except Exception as e:
        print(
            "MEGABOX PAGE CHECK WARNING:",
            type(e).__name__,
        )

    if not baseline_done():
        if not initialize_baseline():
            return

    seen = load_seen()
    status = load_status()

    show_state = status.setdefault(
        "shows",
        {},
    )

    official_state = status.setdefault(
        "official_events",
        {},
    )

    dates = make_dates()
    next_due = stagger_schedule(
        dates,
        start_at=time.monotonic(),
    )

    next_safety_scan_at = (
        next_halfhour_boundary(
            now_kst()
        )
    )

    last_official_event_check = (
        time.monotonic()
        - OFFICIAL_EVENT_CHECK_INTERVAL
    )

    started = time.monotonic()
    heartbeat_started = started

    heartbeat_date_checks = 0
    heartbeat_failed_dates = 0
    heartbeat_new = 0
    heartbeat_official = 0
    heartbeat_error_examples = []

    while (
        time.monotonic() - started
        < RUN_SECONDS
    ):
        now_mono = time.monotonic()
        now_wall = now_kst()

        # ----------------------------------------------------
        # 00/30 FAST SCAN
        # ----------------------------------------------------
        if now_wall >= next_safety_scan_at:
            scan_dates = make_safety_scan_dates()

            events, failed, examples = (
                collect_dates(
                    scan_dates,
                    progress=False,
                )
            )

            heartbeat_date_checks += len(
                scan_dates
            )
            heartbeat_failed_dates += len(
                failed
            )

            for example in examples:
                if (
                    example not in heartbeat_error_examples
                    and len(heartbeat_error_examples) < 2
                ):
                    heartbeat_error_examples.append(
                        example
                    )

            failed_set = set(failed)

            for date in scan_dates:
                if date in failed_set:
                    continue

                date_events = {
                    key: event
                    for key, event in events.items()
                    if event.get("date") == date
                }

                heartbeat_new += send_new_events(
                    date_events,
                    seen,
                )

                merge_date_state(
                    date,
                    date_events,
                    show_state,
                )

                next_due[date] = (
                    time.monotonic()
                    + interval_for_date(date)
                )

            save_seen(seen)
            save_status(status)

            if not failed:
                note_clean_batch()

            next_safety_scan_at = (
                next_halfhour_boundary(
                    now_wall
                    + timedelta(seconds=1)
                )
            )

            continue

        # ----------------------------------------------------
        # 일반 분산 감시
        # ----------------------------------------------------
        due_dates = [
            date
            for date, due_at in sorted(
                next_due.items(),
                key=lambda item: item[1],
            )
            if due_at <= now_mono
        ][:MAX_DUE_DATES_PER_BATCH]

        if due_dates:
            events, failed, examples = (
                collect_dates(
                    due_dates,
                    progress=False,
                )
            )

            heartbeat_date_checks += len(
                due_dates
            )

            heartbeat_failed_dates += len(
                failed
            )

            for example in examples:
                if (
                    example not in heartbeat_error_examples
                    and len(heartbeat_error_examples) < 2
                ):
                    heartbeat_error_examples.append(
                        example
                    )

            failed_set = set(failed)

            for date in due_dates:
                # 실패여도 즉시 재시도하지 않는다.
                # 해당 날짜 원래 주기 뒤에 다시 조회.
                next_due[date] = (
                    time.monotonic()
                    + interval_for_date(date)
                )

                if date in failed_set:
                    continue

                date_events = {
                    key: event
                    for key, event in events.items()
                    if event.get("date") == date
                }

                heartbeat_new += send_new_events(
                    date_events,
                    seen,
                )

                merge_date_state(
                    date,
                    date_events,
                    show_state,
                )

            save_seen(seen)
            save_status(status)

            if not failed:
                note_clean_batch()

            continue

        # ----------------------------------------------------
        # 공식 이벤트 페이지
        # ----------------------------------------------------
        now_mono = time.monotonic()

        if (
            now_mono
            - last_official_event_check
            >= OFFICIAL_EVENT_CHECK_INTERVAL
        ):
            signals = (
                fetch_official_event_signals()
            )

            new_official = (
                process_official_signals(
                    signals,
                    official_state,
                )
            )

            heartbeat_official += (
                new_official
            )

            if new_official:
                save_status(status)

            last_official_event_check = (
                now_mono
            )

        # ----------------------------------------------------
        # 10분 요약
        # ----------------------------------------------------
        now_mono = time.monotonic()

        if (
            now_mono
            - heartbeat_started
            >= HEARTBEAT_INTERVAL
        ):
            mins = max(
                1,
                int(
                    (
                        now_mono
                        - heartbeat_started
                    )
                    // 60
                ),
            )

            counts = count_from_state(
                show_state
            )

            health = (
                "💚 정상 감시중"
                if heartbeat_failed_dates == 0
                else "⚠️ 감시중(일부 API 오류)"
            )

            print(
                f"{health} | "
                f"최근 {mins}분 날짜조회 "
                f"{heartbeat_date_checks}건 | "
                f"메가토크 {counts['메가토크']} | "
                f"무대인사 {counts['무대인사']} | "
                f"DOLBY {counts['DOLBY']} | "
                f"새 일정 {heartbeat_new} | "
                f"공식선행 {heartbeat_official} | "
                f"API실패 {heartbeat_failed_dates}건 | "
                f"현재 요청간격 "
                f"{current_request_gap():.2f}s"
            )

            # 긴 curl 오류 원문 대신 최대 2건만 짧게
            if heartbeat_error_examples:
                print(
                    "⚠️ 오류 예시: "
                    + " || ".join(
                        heartbeat_error_examples[:2]
                    )
                )

            heartbeat_started = now_mono
            heartbeat_date_checks = 0
            heartbeat_failed_dates = 0
            heartbeat_new = 0
            heartbeat_official = 0
            heartbeat_error_examples = []

        # ----------------------------------------------------
        # 다음 일정까지만 짧게 대기
        # ----------------------------------------------------
        next_date_due = (
            min(next_due.values())
            if next_due
            else now_mono + 1.0
        )

        next_official_due = (
            last_official_event_check
            + OFFICIAL_EVENT_CHECK_INTERVAL
        )

        next_heartbeat_due = (
            heartbeat_started
            + HEARTBEAT_INTERVAL
        )

        seconds_to_safety = max(
            0.0,
            (
                next_safety_scan_at
                - now_kst()
            ).total_seconds(),
        )

        next_safety_due = (
            time.monotonic()
            + seconds_to_safety
        )

        next_wake = min(
            next_date_due,
            next_official_due,
            next_heartbeat_due,
            next_safety_due,
        )

        sleep_for = max(
            0.05,
            min(
                1.0,
                next_wake
                - time.monotonic(),
            ),
        )

        time.sleep(
            sleep_for
        )

    save_seen(seen)
    save_status(status)

    print("DONE")


if __name__ == "__main__":
    main()
