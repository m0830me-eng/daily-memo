import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curl_cffi import requests

BRANCH_NO = "1351"
DAYS = 50
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "1800"))

WORKERS = 2

# 전체 요청 시작 간격 자동조절
# 0.18초부터 시작 -> 10사이클 연속 정상일 때 0.01초씩 단축
# 문제 발생 시 0.02초 늘림
START_GAP = 0.18
MIN_GAP = 0.12
MAX_GAP = 0.30
GAP_STEP_DOWN = 0.01
GAP_STEP_UP = 0.02
CLEAN_CYCLES_TO_SPEED_UP = 10

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

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}

_thread_local = threading.local()
_rate_lock = threading.Lock()
_next_request_time = 0.0
_current_gap = START_GAP


def now_kst():
    return datetime.now(KST)


def make_dates():
    today = now_kst()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    ]


def get_session():
    session = getattr(_thread_local, "session", None)

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


def wait_rate_slot():
    global _next_request_time

    with _rate_lock:
        now = time.monotonic()

        if now < _next_request_time:
            wait = _next_request_time - now
            time.sleep(wait)
            now = time.monotonic()

        _next_request_time = now + _current_gap


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
            reset_thread_session()

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None, True, (
                f"{label} {date} ERROR {repr(e)}"
            )

        elapsed = time.monotonic() - started

        if response.status_code in BLOCK_STATUSES:
            reset_thread_session()

            if attempt < 2:
                time.sleep(0.5)
                continue

            return None, True, (
                f"{label} {date} HTTP={response.status_code}"
            )

        if response.status_code != 200:
            return None, True, (
                f"{label} {date} HTTP={response.status_code}"
            )

        try:
            rows = parse_rows(response)
            return rows, False, (
                f"{label} {date} "
                f"HTTP=200 {elapsed:.2f}s "
                f"ROWS={len(rows)}"
            )
        except Exception as e:
            reset_thread_session()
            return None, True, (
                f"{label} {date} JSON ERROR {repr(e)}"
            )

    return None, True, (
        f"{label} {date} UNKNOWN ERROR"
    )


def collect_date(index, date):
    general_rows, general_problem, general_log = (
        request_schedule(
            date,
            special=False,
        )
    )

    dolby_rows, dolby_problem, dolby_log = (
        request_schedule(
            date,
            special=True,
        )
    )

    if general_rows is None:
        general_rows = []

    if dolby_rows is None:
        dolby_rows = []

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
        else:
            counts["DOLBY"] += 1

    return {
        "index": index,
        "date": date,
        "counts": counts,
        "problem": (
            general_problem
            or dolby_problem
        ),
        "logs": [
            general_log,
            dolby_log,
        ],
    }


def scan_50_days():
    dates = make_dates()

    total = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    failed_dates = []
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

        for future in as_completed(futures):
            results.append(
                future.result()
            )

    results.sort(
        key=lambda item: item["index"]
    )

    for item in results:
        print(
            f"--- DATE {item['index']}/50 "
            f"{item['date']} ---"
        )

        for line in item["logs"]:
            print(line)

        for key in total:
            total[key] += (
                item["counts"][key]
            )

        if item["problem"]:
            failed_dates.append(
                item["date"]
            )

    return total, failed_dates


def main():
    global _current_gap
    global _next_request_time

    print("=" * 76)
    print("MEGABOX COEX 50-DAY SAFE PARALLEL TEST")
    print("=" * 76)
    print("BRANCH: 메가박스 코엑스 / 1351")
    print("TARGET: 메가토크 / 무대인사 / DOLBY CINEMA")
    print("SCAN: TODAY ~ +49 DAYS / 50 DAYS FULL SCAN")
    print("WORKERS: 2 FIXED")
    print("START REQUEST GAP: 0.18s")
    print("MIN REQUEST GAP: 0.12s")
    print("RULE: 10 CLEAN CYCLES -> -0.01s gap")
    print("PROBLEM: +0.02s gap")
    print("RUN SECONDS:", RUN_SECONDS)
    print("NO DISCORD / NO STATE FILE")
    print("=" * 76)

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

    cycle = 0
    clean_streak = 0
    started = time.monotonic()
    stats = {}

    while (
        time.monotonic() - started
        < RUN_SECONDS
    ):
        cycle += 1
        cycle_started = time.monotonic()

        with _rate_lock:
            _next_request_time = (
                time.monotonic()
            )

        print()
        print("=" * 76)
        print(
            f"CYCLE #{cycle} | "
            f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST | "
            f"GAP={_current_gap:.2f}s"
        )
        print("50 DAYS SAFE PARALLEL FULL SCAN START")
        print("=" * 76)

        counts, failed_dates = (
            scan_50_days()
        )

        elapsed = (
            time.monotonic()
            - cycle_started
        )

        problem = bool(failed_dates)

        key = round(
            _current_gap,
            2,
        )

        item = stats.setdefault(
            key,
            {
                "cycles": 0,
                "clean": 0,
                "problem": 0,
                "min_scan": None,
                "max_scan": 0.0,
            },
        )

        item["cycles"] += 1
        item["max_scan"] = max(
            item["max_scan"],
            elapsed,
        )

        if item["min_scan"] is None:
            item["min_scan"] = elapsed
        else:
            item["min_scan"] = min(
                item["min_scan"],
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
                ", ".join(
                    failed_dates
                ),
            )

        if problem:
            item["problem"] += 1
            clean_streak = 0

            old_gap = _current_gap
            _current_gap = min(
                MAX_GAP,
                round(
                    _current_gap
                    + GAP_STEP_UP,
                    2,
                ),
            )

            print(
                "RESULT: PROBLEM",
                f"GAP {old_gap:.2f}s "
                f"-> {_current_gap:.2f}s"
            )

        else:
            item["clean"] += 1
            clean_streak += 1

            print(
                "RESULT: CLEAN",
                f"STREAK={clean_streak}/"
                f"{CLEAN_CYCLES_TO_SPEED_UP}",
            )

            if (
                clean_streak
                >= CLEAN_CYCLES_TO_SPEED_UP
                and _current_gap
                > MIN_GAP
            ):
                old_gap = _current_gap
                _current_gap = max(
                    MIN_GAP,
                    round(
                        _current_gap
                        - GAP_STEP_DOWN,
                        2,
                    ),
                )
                clean_streak = 0

                print(
                    "SPEED UP:",
                    f"GAP {old_gap:.2f}s "
                    f"-> {_current_gap:.2f}s"
                )

        print(
            "WAIT: 0s "
            "(next 50-day scan starts immediately)"
        )

    print()
    print("=" * 76)
    print("TEST SUMMARY")
    print("=" * 76)

    for gap in sorted(
        stats.keys(),
        reverse=True,
    ):
        item = stats[gap]
        print(
            f"GAP={gap:.2f}s | "
            f"CYCLES={item['cycles']} | "
            f"CLEAN={item['clean']} | "
            f"PROBLEM={item['problem']} | "
            f"MIN_SCAN={item['min_scan']:.2f}s | "
            f"MAX_SCAN={item['max_scan']:.2f}s"
        )

    print(
        "FINAL REQUEST GAP:",
        f"{_current_gap:.2f}s"
    )
    print("DONE")


if __name__ == "__main__":
    main()
