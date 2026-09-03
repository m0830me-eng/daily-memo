import os
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
