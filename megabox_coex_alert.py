import os
import json
import time
import html
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curl_cffi import requests


# ============================================================
# MEGABOX COEX MONITOR
# 감지 대상:
# - 메가토크
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
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))

KST = ZoneInfo("Asia/Seoul")

SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

STATE_FILE = "seen_megabox_coex.json"
BASELINE_FILE = "baseline_megabox_coex.done"

# 기존 14일 baseline을 자동으로 무효화해서
# 업그레이드 첫 실행 때 50일 전체를 조용히 새 기준값으로 등록한다.
BASELINE_SCHEMA = "MEGABOX_COEX_50DAYS_2WORKERS_GAP017_V1"

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

        print(
            "STATE SAVED:",
            len(seen),
        )

    except Exception as e:
        print(
            "STATE SAVE ERROR:",
            repr(e),
        )


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


def wait_rate_slot():
    global _next_request_time

    with _rate_lock:
        now = time.monotonic()

        if now < _next_request_time:
            time.sleep(
                _next_request_time - now
            )
            now = time.monotonic()

        _next_request_time = (
            now + REQUEST_GAP
        )


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
            reset_thread_session()

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None, True, last_error

        if response.status_code != 200:
            return None, True, (
                f"{label} {date} "
                f"HTTP={response.status_code}"
            )

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

            reset_thread_session()

            # HTTP 200이어도 비정상/빈 응답이면 한 번 새 세션으로 재시도.
            if attempt < 2:
                time.sleep(0.5)
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
    return "메가토크" in all_text(row)


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


def normalize_event(
    date,
    row,
    event_type,
):
    return {
        "date": date,
        "type": event_type,
        "movie": get_movie(row),
        "start": get_start(row),
        "end": get_end(row),
        "screen": get_screen(row),
        "schedule_no": get_schedule_no(row),
        "link": make_booking_link(row),
    }


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


def collect_all_50_days():
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

    for item in results:
        print(
            f"--- DATE {item['index']}/50 "
            f"{item['date']} ---"
        )

        for line in item["logs"]:
            print(line)

        all_events.update(
            item["events"]
        )

        if item["problem"]:
            failed_dates.append(
                item["date"]
            )

    # 중복 실패 날짜 제거 + 날짜순 정렬
    failed_dates = sorted(
        set(failed_dates)
    )

    return all_events, failed_dates


# ============================================================
# Discord grouped notification
# ============================================================

def send_new_events(
    events,
    seen,
):
    new_events = [
        (key, event)
        for key, event in events.items()
        if key not in seen
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
    ), items in sorted(
        groups.items()
    ):
        if event_type == "DOLBY":
            title = (
                "🎉 메가박스 코엑스 "
                "DOLBY CINEMA 새 상영 일정"
            )

        elif event_type == "무대인사":
            title = (
                "🎉 메가박스 코엑스 "
                "무대인사 새 상영 일정"
            )

        else:
            title = (
                "🎉 메가박스 코엑스 "
                "메가토크 새 상영 일정"
            )

        lines = []

        if DISCORD_USER_ID:
            lines.append(
                f"<@{DISCORD_USER_ID}>"
            )
            lines.append("")

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

            time_text = (
                f"{start}~{end}"
                if end
                else start
            )

            movie = event["movie"]
            screen = event["screen"]

            linked_movie = (
                f"[{movie}]"
                f"({event['link']})"
            )

            if screen:
                lines.append(
                    f"• {time_text} — "
                    f"{linked_movie} | "
                    f"코엑스 / {screen}"
                )
            else:
                lines.append(
                    f"• {time_text} — "
                    f"{linked_movie}"
                )

        message = "\n".join(lines)

        print(
            "NEW EVENT GROUP:",
            event_type,
            date,
            "COUNT=",
            len(items),
        )

        if send_discord(message):
            for key, _ in items:
                seen.add(key)
                sent_count += 1

    return sent_count


# ============================================================
# Diagnostics
# ============================================================

def print_counts(events):
    counts = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    for event in events.values():
        event_type = event.get(
            "type",
            "",
        )

        if event_type in counts:
            counts[event_type] += 1

    print(
        "MEGATALK COUNT:",
        counts["메가토크"],
    )

    print(
        "무대인사 COUNT:",
        counts["무대인사"],
    )

    print(
        "DOLBY COUNT:",
        counts["DOLBY"],
    )


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
        "메가토크 / 무대인사 / DOLBY CINEMA"
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
        f"{REQUEST_GAP:.2f}s FIXED",
    )

    print(
        "NEXT CYCLE: "
        "IMMEDIATELY AFTER EACH 50-DAY SCAN"
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
            "현재 50일 전체 메가토크 / 무대인사 / DOLBY를 "
            "알림 없이 기준값으로 등록합니다."
        )

        events, failed_dates = (
            collect_all_50_days()
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

        print(
            "BASELINE EVENT COUNT:",
            len(seen),
        )

        print_counts(events)

        save_seen(seen)
        mark_baseline_done()

        print("BASELINE COMPLETE")
        print(
            "이번 실행에서는 "
            "Discord 알림을 보내지 않았습니다."
        )
        return

    # --------------------------------------------------------
    # 정상 감시
    # --------------------------------------------------------

    seen = load_seen()
    monitor_started = time.monotonic()
    cycle_number = 0

    while True:
        total_elapsed = (
            time.monotonic()
            - monitor_started
        )

        if total_elapsed >= RUN_SECONDS:
            print(
                "MONITOR TIME FINISHED"
            )
            break

        cycle_number += 1
        cycle_started = time.monotonic()

        print()
        print("=" * 72)
        print(
            f"CYCLE #{cycle_number} | "
            f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST | "
            f"GAP={REQUEST_GAP:.2f}s"
        )
        print(
            "50 DAYS / 2 WORKERS FULL SCAN START"
        )
        print("=" * 72)

        events, failed_dates = (
            collect_all_50_days()
        )

        print()
        print(
            "FULL EVENT COUNT:",
            len(events),
        )

        print_counts(events)

        if failed_dates:
            print(
                "FAILED DATES:",
                ", ".join(
                    failed_dates
                ),
            )

        new_count = send_new_events(
            events,
            seen,
        )

        print(
            "NEW EVENT COUNT:",
            new_count,
        )

        save_seen(seen)

        cycle_elapsed = (
            time.monotonic()
            - cycle_started
        )

        print(
            "CYCLE ELAPSED:",
            f"{cycle_elapsed:.2f}s",
        )

        print(
            "WAIT: 0s "
            "(next 50-day scan starts immediately)"
        )

    save_seen(seen)

    print(
        "FINAL SEEN STATE:",
        len(seen),
    )

    print("DONE")


if __name__ == "__main__":
    main()
