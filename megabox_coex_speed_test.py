import os
import time

import megabox_coex_alert as monitor


RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "1800"))

START_INTERVAL = 15.0
MIN_INTERVAL = 10.0
MAX_INTERVAL = 30.0
SPEED_STEP = 1.0
CLEAN_CYCLES_TO_SPEED_UP = 20

BLOCK_STATUSES = {403, 429, 503}


class TrackingSession:
    def __init__(self):
        self.inner = monitor.requests.Session(
            impersonate="chrome"
        )
        self.cycle_problem = False
        self.problem_details = []

    def reset_cycle(self):
        self.cycle_problem = False
        self.problem_details = []

    def get(self, *args, **kwargs):
        try:
            response = self.inner.get(
                *args,
                **kwargs,
            )
            if response.status_code in BLOCK_STATUSES:
                self.cycle_problem = True
                self.problem_details.append(
                    f"GET HTTP {response.status_code}"
                )
            return response
        except Exception as e:
            self.cycle_problem = True
            self.problem_details.append(
                f"GET ERROR {repr(e)}"
            )
            raise

    def post(self, *args, **kwargs):
        try:
            response = self.inner.post(
                *args,
                **kwargs,
            )
            if response.status_code in BLOCK_STATUSES:
                self.cycle_problem = True
                self.problem_details.append(
                    f"POST HTTP {response.status_code}"
                )
            return response
        except Exception as e:
            self.cycle_problem = True
            self.problem_details.append(
                f"POST ERROR {repr(e)}"
            )
            raise


def event_counts(events):
    counts = {
        "메가토크": 0,
        "무대인사": 0,
        "DOLBY": 0,
    }

    for event in events.values():
        kind = event.get("type", "")
        if kind in counts:
            counts[kind] += 1

    return counts


def main():
    print("=" * 68)
    print("MEGABOX COEX AUTO SPEED TEST")
    print("=" * 68)
    print("BRANCH: 메가박스 코엑스 / 1351")
    print("TARGET: 메가토크 / 무대인사 / DOLBY CINEMA")
    print("SCAN: TODAY ~ +13 DAYS / 14 DAYS FULL SCAN")
    print("START INTERVAL:", START_INTERVAL, "seconds")
    print("MIN INTERVAL:", MIN_INTERVAL, "seconds")
    print(
        "RULE:",
        f"{CLEAN_CYCLES_TO_SPEED_UP} CLEAN CYCLES -> -1s"
    )
    print(
        "BLOCK:",
        "403 / 429 / 503 / request error -> +1s"
    )
    print("RUN SECONDS:", RUN_SECONDS)
    print(
        "NOTE: Discord/state files are NOT used in this test."
    )
    print("=" * 68)

    session = TrackingSession()

    try:
        r = session.get(
            "https://www.megabox.co.kr/",
            headers=monitor.HEADERS,
            timeout=8,
        )
        print("MEGABOX PAGE STATUS:", r.status_code)
    except Exception as e:
        print("PAGE CHECK ERROR:", repr(e))

    current_interval = START_INTERVAL
    clean_streak = 0
    cycle_number = 0
    started = time.monotonic()

    interval_stats = {}

    while True:
        total_elapsed = time.monotonic() - started

        if total_elapsed >= RUN_SECONDS:
            break

        cycle_number += 1
        cycle_started = time.monotonic()
        session.reset_cycle()

        print()
        print("=" * 68)
        print(
            f"CYCLE #{cycle_number} | "
            f"{monitor.now_kst().strftime('%Y-%m-%d %H:%M:%S')} KST | "
            f"TARGET_INTERVAL={current_interval:.0f}s"
        )
        print("=" * 68)

        events, failed_dates = (
            monitor.collect_all_14_days(session)
        )

        counts = event_counts(events)

        cycle_elapsed = (
            time.monotonic() - cycle_started
        )

        problem = (
            session.cycle_problem
            or bool(failed_dates)
        )

        key = int(current_interval)
        stats = interval_stats.setdefault(
            key,
            {
                "cycles": 0,
                "clean": 0,
                "problem": 0,
                "max_elapsed": 0.0,
            },
        )

        stats["cycles"] += 1
        stats["max_elapsed"] = max(
            stats["max_elapsed"],
            cycle_elapsed,
        )

        print()
        print(
            "EVENTS:",
            f"TOTAL={len(events)}",
            f"MEGATALK={counts['메가토크']}",
            f"STAGE={counts['무대인사']}",
            f"DOLBY={counts['DOLBY']}",
        )
        print(
            "CYCLE ELAPSED:",
            f"{cycle_elapsed:.2f}s",
        )

        if failed_dates:
            print(
                "FAILED DATES:",
                ", ".join(failed_dates),
            )

        if session.problem_details:
            print(
                "PROBLEM DETAILS:",
                " | ".join(
                    session.problem_details
                ),
            )

        if problem:
            stats["problem"] += 1
            clean_streak = 0

            old_interval = current_interval
            current_interval = min(
                MAX_INTERVAL,
                current_interval + SPEED_STEP,
            )

            print(
                "RESULT: PROBLEM",
                f"-> {old_interval:.0f}s "
                f"to {current_interval:.0f}s"
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
                clean_streak
                >= CLEAN_CYCLES_TO_SPEED_UP
                and current_interval
                > MIN_INTERVAL
            ):
                old_interval = current_interval
                current_interval = max(
                    MIN_INTERVAL,
                    current_interval - SPEED_STEP,
                )
                clean_streak = 0

                print(
                    "SPEED UP:",
                    f"{old_interval:.0f}s "
                    f"-> {current_interval:.0f}s"
                )

        sleep_time = (
            current_interval - cycle_elapsed
        )

        if sleep_time > 0:
            print(
                "WAIT:",
                f"{sleep_time:.2f}s"
            )
            time.sleep(sleep_time)
        else:
            print(
                "WAIT: 0s "
                "(full scan already took longer "
                "than target interval)"
            )

    print()
    print("=" * 68)
    print("TEST SUMMARY")
    print("=" * 68)

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
            f"MAX_SCAN={stats['max_elapsed']:.2f}s"
        )

    print(
        "FINAL TEST INTERVAL:",
        f"{current_interval:.0f}s"
    )
    print("DONE")


if __name__ == "__main__":
    main()
