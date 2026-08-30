import os
import json
import time
import html
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
# 핵심 동작:
# - 한국시간 기준 오늘~13일 뒤 = 14일 전체
# - 14일 전체를 한 사이클로 검사
# - 사이클 시작 기준 10초마다 다시 14일 전체 검사
# ============================================================

BRANCH_NO = "1351"
BRANCH_NAME = "메가박스 코엑스"

DAYS = 14
CHECK_INTERVAL = 10
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))

KST = ZoneInfo("Asia/Seoul")

SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

STATE_FILE = "seen_megabox_coex.json"
BASELINE_FILE = "baseline_megabox_coex.done"

DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_MEGABOX_COEX",
    "",
).strip()

DISCORD_USER_ID = "1383846907847381184"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": (
        "https://www.megabox.co.kr/"
        "theater/time?brchNo=1351"
    ),
    "X-Requested-With": "XMLHttpRequest",
}


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

    try:
        response = requests.post(
            DISCORD_WEBHOOK,
            json={
                "content": message,
                "flags": 4,  # Discord 링크 미리보기(Embed) 숨김
                "allowed_mentions": {
                    "users": [DISCORD_USER_ID]
                },
            },
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
    return os.path.exists(BASELINE_FILE)


def mark_baseline_done():
    with open(
        BASELINE_FILE,
        "w",
        encoding="utf-8",
    ) as f:
        f.write(now_kst().isoformat())

    print("BASELINE MARKER CREATED")


# ============================================================
# API helpers
# ============================================================

def parse_json_response(response):
    try:
        return response.json()

    except Exception as e:
        print(
            "JSON ERROR:",
            repr(e),
        )
        print(
            "RESPONSE PREVIEW:",
            response.text[:300],
        )
        return {}


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
    session,
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

    for attempt in range(1, 3):
        started = time.monotonic()

        try:
            response = session.post(
                SCHEDULE_API,
                data=params,
                headers=HEADERS,
                timeout=8,
            )

        except Exception as e:
            print(
                f"{label} API {date} "
                f"ATTEMPT={attempt} ERROR:",
                repr(e),
            )

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None

        elapsed = time.monotonic() - started

        print(
            f"{label} API {date} "
            f"STATUS={response.status_code} "
            f"TIME={elapsed:.2f}s "
            f"SIZE={len(response.content):,} bytes"
        )

        if response.status_code in (
            403, 429, 500, 502, 503, 504,
        ):
            print(
                f"{label} API TEMPORARY ERROR "
                f"{response.status_code}"
            )

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None

        if response.status_code != 200:
            print(
                f"{label} API UNEXPECTED STATUS "
                f"{response.status_code}"
            )
            return None

        data = parse_json_response(response)

        return extract_movie_form_list(data)

    return None


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
# Collect
# ============================================================

def collect_date(
    session,
    date,
):
    events = {}

    general_rows = request_schedule(
        session,
        date,
        special=False,
    )

    if general_rows is None:
        print(
            "GENERAL COLLECTION FAILED:",
            date,
        )
    else:
        print(
            "GENERAL ROW COUNT:",
            len(general_rows),
        )

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

    dolby_rows = request_schedule(
        session,
        date,
        special=True,
    )

    if dolby_rows is None:
        print(
            "DOLBY COLLECTION FAILED:",
            date,
        )
    else:
        print(
            "DOLBY ROW COUNT:",
            len(dolby_rows),
        )

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

    if (
        general_rows is None
        and dolby_rows is None
    ):
        return None

    return events


def collect_all_14_days(
    session,
):
    all_events = {}
    failed_dates = []

    dates = make_dates()

    for index, date in enumerate(
        dates,
        start=1,
    ):
        print()
        print(
            f"--- DATE {index}/{len(dates)} "
            f"{date} ---"
        )

        events = collect_date(
            session,
            date,
        )

        if events is None:
            failed_dates.append(date)
            continue

        all_events.update(events)

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

        lines = [
            f"<@{DISCORD_USER_ID}>",
            "",
            f"**{title}**",
            "",
            f"📅 **{pretty_date(date)}**",
        ]

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
    print("=" * 60)
    print("MEGABOX COEX MONITOR")
    print("=" * 60)

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
        "TODAY ~ +13 DAYS (14 DAYS TOTAL)"
    )

    print(
        "SCAN MODE: "
        "ALL 14 DAYS EVERY 10 SECONDS"
    )

    print(
        "CYCLE INTERVAL:",
        CHECK_INTERVAL,
        "seconds",
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

    print("=" * 60)

    session = requests.Session(
        impersonate="chrome"
    )

    try:
        r = session.get(
            "https://www.megabox.co.kr/",
            headers=HEADERS,
            timeout=5,
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
    # 최초 기준값
    # --------------------------------------------------------

    if not baseline_done():
        print()
        print("=" * 60)
        print("INITIAL BASELINE")
        print("=" * 60)

        events, failed_dates = (
            collect_all_14_days(
                session
            )
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
    monitor_started = (
        time.monotonic()
    )
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
        cycle_started = (
            time.monotonic()
        )

        print()
        print("=" * 60)
        print(
            f"CYCLE #{cycle_number} "
            f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST"
        )
        print(
            "14 DAYS FULL SCAN START"
        )
        print("=" * 60)

        events, failed_dates = (
            collect_all_14_days(
                session
            )
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

        sleep_time = (
            CHECK_INTERVAL
            - cycle_elapsed
        )

        if sleep_time > 0:
            print(
                f"WAIT {sleep_time:.2f}s "
                "UNTIL NEXT FULL SCAN"
            )
            time.sleep(
                sleep_time
            )
        else:
            print(
                "FULL SCAN TOOK >= 10s - "
                "START NEXT CYCLE IMMEDIATELY"
            )

    save_seen(seen)

    print(
        "FINAL SEEN STATE:",
        len(seen),
    )

    print("DONE")


if __name__ == "__main__":
    main()