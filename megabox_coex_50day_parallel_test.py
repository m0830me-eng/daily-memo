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

START_WORKERS = 2
MAX_WORKERS = 4
CLEAN_CYCLES_TO_SPEED_UP = 20

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


def request_schedule(date, special=False):
    session = get_session()

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
            if attempt < 2:
                time.sleep(0.5)
                continue
            return None, True, (
                f"{label} {date} ERROR {repr(e)}"
            )

        elapsed = time.monotonic() - started

        if response.status_code in BLOCK_STATUSES:
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
            return None, True, (
                f"{label} {date} JSON ERROR {repr(e)}"
            )

    return None, True, f"{label} {date} UNKNOWN ERROR"


def collect_date(index, date):
    general_rows, general_problem, general_log = (
        request_schedule(date, special=False)
    )
    dolby_rows, dolby_problem, dolby_log = (
        request_schedule(date, special=True)
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
        elif is_dolby(row):
            counts["DOLBY"] += 1
        else:
            counts["DOLBY"] += 1

    return {
        "index": index,
        "date": date,
        "counts": counts,
        "problem": general_problem or dolby_problem,
        "logs": [general_log, dolby_log],
    }


def scan_50_days(workers):
    dates = make_dates()

    total = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    failed_dates = []
    results = []

    with ThreadPoolExecutor(
        max_workers=workers
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
            total[key] += item["counts"][key]

        if item["problem"]:
            failed_dates.append(
                item["date"]
            )

    return total, failed_dates


def main():
    print("=" * 74)
    print("MEGABOX COEX 50-DAY PARALLEL SPEED TEST")
    print("=" * 74)
    print("BRANCH: 메가박스 코엑스 / 1351")
    print("TARGET: 메가토크 / 무대인사 / DOLBY CINEMA")
    print("SCAN: TODAY ~ +49 DAYS / 50 DAYS FULL SCAN")
    print("PARALLEL: 2 workers -> 3 -> 4")
    print("RULE: 20 CLEAN CYCLES -> +1 worker")
    print("RUN SECONDS:", RUN_SECONDS)
    print("NO DISCORD / NO STATE FILE")
    print("=" * 74)

    # 기본 연결 확인
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

    workers = START_WORKERS
    clean_streak = 0
    cycle = 0
    started = time.monotonic()

    stats = {}

    while time.monotonic() - started < RUN_SECONDS:
        cycle += 1
        cycle_started = time.monotonic()

        print()
        print("=" * 74)
        print(
            f"CYCLE #{cycle} | "
            f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST | "
            f"WORKERS={workers}"
        )
        print("50 DAYS PARALLEL FULL SCAN START")
        print("=" * 74)

        counts, failed_dates = (
            scan_50_days(workers)
        )

        elapsed = (
            time.monotonic() - cycle_started
        )

        problem = bool(failed_dates)

        worker_stats = stats.setdefault(
            workers,
            {
                "cycles": 0,
                "clean": 0,
                "problem": 0,
                "min_scan": None,
                "max_scan": 0.0,
            },
        )

        worker_stats["cycles"] += 1
        worker_stats["max_scan"] = max(
            worker_stats["max_scan"],
            elapsed,
        )

        if worker_stats["min_scan"] is None:
            worker_stats["min_scan"] = elapsed
        else:
            worker_stats["min_scan"] = min(
                worker_stats["min_scan"],
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
            worker_stats["problem"] += 1
            clean_streak = 0

            # 문제 발생 시 한 단계 낮춤
            old_workers = workers
            workers = max(
                1,
                workers - 1,
            )

            print(
                "RESULT: PROBLEM",
                f"WORKERS {old_workers} -> {workers}"
            )

        else:
            worker_stats["clean"] += 1
            clean_streak += 1

            print(
                "RESULT: CLEAN",
                f"STREAK={clean_streak}/"
                f"{CLEAN_CYCLES_TO_SPEED_UP}",
            )

            if (
                clean_streak
                >= CLEAN_CYCLES_TO_SPEED_UP
                and workers < MAX_WORKERS
            ):
                old_workers = workers
                workers += 1
                clean_streak = 0

                print(
                    "SPEED UP:",
                    f"WORKERS {old_workers} -> {workers}"
                )

        # 병렬 테스트는 가능한 최속 연속 스캔을 측정하므로
        # 인위적 대기 없이 다음 50일 스캔 시작
        print(
            "WAIT: 0s "
            "(next 50-day parallel scan starts immediately)"
        )

    print()
    print("=" * 74)
    print("TEST SUMMARY")
    print("=" * 74)

    for worker_count in sorted(stats):
        item = stats[worker_count]
        print(
            f"WORKERS={worker_count} | "
            f"CYCLES={item['cycles']} | "
            f"CLEAN={item['clean']} | "
            f"PROBLEM={item['problem']} | "
            f"MIN_SCAN={item['min_scan']:.2f}s | "
            f"MAX_SCAN={item['max_scan']:.2f}s"
        )

    print(
        "FINAL WORKERS:",
        workers,
    )
    print("DONE")


if __name__ == "__main__":
    main()
