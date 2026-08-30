import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curl_cffi import requests

BRANCH_NO = "1351"
BRANCH_NAME = "메가박스 코엑스"
DAYS = 50

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "1800"))

START_INTERVAL = 15.0
MIN_INTERVAL = 10.0
MAX_INTERVAL = 30.0
STEP = 1.0
CLEAN_CYCLES_TO_SPEED_UP = 20

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}

KST = ZoneInfo("Asia/Seoul")

SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://www.megabox.co.kr/theater/time?brchNo=1351",
    "Origin": "https://www.megabox.co.kr",
    "X-Requested-With": "XMLHttpRequest",
}


def now_kst():
    return datetime.now(KST)


def make_dates():
    today = now_kst()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    ]


def parse_rows(response):
    data = response.json()
    mega_map = data.get("megaMap") or {}
    rows = mega_map.get("movieFormList")
    if isinstance(rows, list):
        return rows

    for value in data.values():
        if isinstance(value, dict):
            rows = value.get("movieFormList")
            if isinstance(rows, list):
                return rows

    return []


def all_text(row):
    return " ".join(
        f"{k}={v}"
        for k, v in row.items()
        if v is not None
    )


def is_stage(row):
    text = all_text(row)
    return "무대인사" in text or "舞台挨拶" in text


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


def request_schedule(session, date, special=False):
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
                f"ATTEMPT={attempt} ERROR: {repr(e)}"
            )
            if attempt < 2:
                time.sleep(0.5)
                continue
            return None, True

        elapsed = time.monotonic() - started

        print(
            f"{label} API {date} "
            f"STATUS={response.status_code} "
            f"TIME={elapsed:.2f}s "
            f"SIZE={len(response.content):,} bytes"
        )

        if response.status_code in BLOCK_STATUSES:
            if attempt < 2:
                time.sleep(0.5)
                continue
            return None, True

        if response.status_code != 200:
            return None, True

        try:
            return parse_rows(response), False
        except Exception as e:
            print(
                f"{label} JSON ERROR {date}: {repr(e)}"
            )
            return None, True

    return None, True


def collect_date(session, date):
    problem = False

    general_rows, general_problem = request_schedule(
        session, date, special=False
    )
    dolby_rows, dolby_problem = request_schedule(
        session, date, special=True
    )

    problem = general_problem or dolby_problem

    if general_rows is None:
        print("GENERAL COLLECTION FAILED:", date)
        general_rows = []
    else:
        print("GENERAL ROW COUNT:", len(general_rows))

    if dolby_rows is None:
        print("DOLBY COLLECTION FAILED:", date)
        dolby_rows = []
    else:
        print("DOLBY ROW COUNT:", len(dolby_rows))

    counts = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    for row in general_rows:
        if is_stage(row):
            counts["무대인사"] += 1
        elif is_megatalk(row):
            counts["메가토크"] += 1

    for row in dolby_rows:
        if is_stage(row):
            counts["무대인사"] += 1
        elif is_megatalk(row):
            counts["메가토크"] += 1
        elif is_dolby(row):
            counts["DOLBY"] += 1
        else:
            counts["DOLBY"] += 1

    return counts, problem


def scan_50_days(session):
    total = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }
    failed_dates = []

    dates = make_dates()

    for index, date in enumerate(dates, start=1):
        print()
        print(
            f"--- DATE {index}/50 {date} ---"
        )

        counts, problem = collect_date(
            session,
            date,
        )

        for key in total:
            total[key] += counts[key]

        if problem:
            failed_dates.append(date)

    return total, failed_dates


def main():
    print("=" * 72)
    print("MEGABOX COEX 50-DAY AUTO SPEED TEST")
    print("=" * 72)
    print("BRANCH: 메가박스 코엑스 / 1351")
    print("TARGET: 메가토크 / 무대인사 / DOLBY CINEMA")
    print("SCAN: TODAY ~ +49 DAYS / 50 DAYS FULL SCAN")
    print("START INTERVAL: 15.0 seconds")
    print("MIN INTERVAL: 10.0 seconds")
    print("RULE: 20 CLEAN CYCLES -> -1s")
    print("RUN SECONDS:", RUN_SECONDS)
    print("NO DISCORD / NO STATE FILE")
    print("=" * 72)

    session = requests.Session(
        impersonate="chrome"
    )

    try:
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

    current_interval = START_INTERVAL
    clean_streak = 0
    cycle_number = 0
    started = time.monotonic()
    interval_stats = {}

    while time.monotonic() - started < RUN_SECONDS:
        cycle_number += 1
        cycle_started = time.monotonic()

        print()
        print("=" * 72)
        print(
            f"CYCLE #{cycle_number} | "
            f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST | "
            f"TARGET_INTERVAL={current_interval:.0f}s"
        )
        print("50 DAYS FULL SCAN START")
        print("=" * 72)

        counts, failed_dates = scan_50_days(
            session
        )

        elapsed = (
            time.monotonic() - cycle_started
        )

        problem = bool(failed_dates)

        key = int(current_interval)
        stats = interval_stats.setdefault(
            key,
            {
                "cycles": 0,
                "clean": 0,
                "problem": 0,
                "min_scan": None,
                "max_scan": 0.0,
            },
        )

        stats["cycles"] += 1
        stats["max_scan"] = max(
            stats["max_scan"],
            elapsed,
        )

        if stats["min_scan"] is None:
            stats["min_scan"] = elapsed
        else:
            stats["min_scan"] = min(
                stats["min_scan"],
                elapsed,
            )

        print()
        print(
            "EVENTS:",
            f"MEGATALK={counts['메가토크']}",
            f"STAGE={counts['무대인사']}",
            f"DOLBY={counts['DOLBY']}",
        )
        print(
            "CYCLE ELAPSED:",
            f"{elapsed:.2f}s",
        )

        if failed_dates:
            print(
                "FAILED DATES:",
                ", ".join(failed_dates),
            )

        if problem:
            stats["problem"] += 1
            clean_streak = 0

            old_interval = current_interval
            current_interval = min(
                MAX_INTERVAL,
                current_interval + STEP,
            )

            print(
                "RESULT: PROBLEM",
                f"-> {old_interval:.0f}s "
                f"to {current_interval:.0f}s",
            )

        else:
            stats["clean"] += 1
            clean_streak += 1

            print(
                "RESULT: CLEAN",
                f"STREAK={clean_streak}/"
                f"{CLEAN_CYCLES_TO_SPEED_UP}",
            )

            if (
                clean_streak >= CLEAN_CYCLES_TO_SPEED_UP
                and current_interval > MIN_INTERVAL
            ):
                old_interval = current_interval
                current_interval = max(
                    MIN_INTERVAL,
                    current_interval - STEP,
                )
                clean_streak = 0

                print(
                    "SPEED UP:",
                    f"{old_interval:.0f}s "
                    f"-> {current_interval:.0f}s",
                )

        wait = max(
            0.0,
            current_interval - elapsed,
        )

        if wait > 0:
            print(
                "WAIT:",
                f"{wait:.2f}s",
            )
            time.sleep(wait)
        else:
            print(
                "WAIT: 0s "
                "(50-day scan already took longer than target interval)"
            )

    print()
    print("=" * 72)
    print("TEST SUMMARY")
    print("=" * 72)

    for interval in sorted(
        interval_stats.keys(),
        reverse=True,
    ):
        stats = interval_stats[interval]
        print(
            f"{interval}s | "
            f"CYCLES={stats['cycles']} | "
            f"CLEAN={stats['clean']} | "
            f"PROBLEM={stats['problem']} | "
            f"MIN_SCAN={stats['min_scan']:.2f}s | "
            f"MAX_SCAN={stats['max_scan']:.2f}s"
        )

    print(
        "FINAL TEST INTERVAL:",
        f"{current_interval:.0f}s"
    )
    print("DONE")


if __name__ == "__main__":
    main()
