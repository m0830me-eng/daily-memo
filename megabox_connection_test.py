import socket
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from curl_cffi import requests


HOST = "www.megabox.co.kr"
BRANCH_NO = "1351"
KST = ZoneInfo("Asia/Seoul")

HOME_URL = "https://www.megabox.co.kr/"
SCHEDULE_API = (
    "https://www.megabox.co.kr/"
    "on/oh/ohc/Brch/schedulePage.do"
)

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


def today():
    return datetime.now(KST).strftime("%Y%m%d")


def print_result(label, started, response=None, error=None):
    elapsed = time.monotonic() - started

    if error is not None:
        print(
            f"{label}: ERROR | "
            f"{elapsed:.2f}s | {repr(error)}",
            flush=True,
        )
        return

    print(
        f"{label}: HTTP={response.status_code} | "
        f"{elapsed:.2f}s | "
        f"SIZE={len(response.content):,}",
        flush=True,
    )

    preview = (response.text or "")[:160].replace(
        "\n", " "
    )
    print(
        f"{label} PREVIEW: {preview}",
        flush=True,
    )


def main():
    print("=" * 68)
    print("MEGABOX COEX CONNECTION DIAGNOSTIC")
    print("=" * 68)
    print("KST:", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    print("DATE:", today())
    print("NO DISCORD / NO STATE FILE")
    print("=" * 68)

    # 1) DNS
    started = time.monotonic()
    try:
        infos = socket.getaddrinfo(
            HOST,
            443,
            type=socket.SOCK_STREAM,
        )
        ips = sorted({
            item[4][0]
            for item in infos
        })
        print(
            "DNS: OK | "
            f"{time.monotonic() - started:.2f}s | "
            + ", ".join(ips[:8]),
            flush=True,
        )
    except Exception as e:
        print(
            "DNS: ERROR | "
            f"{time.monotonic() - started:.2f}s | "
            f"{repr(e)}",
            flush=True,
        )

    # 2) TCP 443
    started = time.monotonic()
    try:
        with socket.create_connection(
            (HOST, 443),
            timeout=8,
        ):
            pass

        print(
            "TCP 443: OK | "
            f"{time.monotonic() - started:.2f}s",
            flush=True,
        )
    except Exception as e:
        print(
            "TCP 443: ERROR | "
            f"{time.monotonic() - started:.2f}s | "
            f"{repr(e)}",
            flush=True,
        )

    session = requests.Session(
        impersonate="chrome"
    )

    # 3) Homepage with generous timeout
    started = time.monotonic()
    try:
        r = session.get(
            HOME_URL,
            headers=HEADERS,
            timeout=20,
        )
        print_result(
            "HOME GET",
            started,
            response=r,
        )
    except Exception as e:
        print_result(
            "HOME GET",
            started,
            error=e,
        )

    base = {
        "masterType": "brch",
        "brchNo": BRANCH_NO,
        "firstAt": "N",
        "brchNo1": BRANCH_NO,
        "crtDe": today(),
        "playDe": today(),
    }

    # 4) General schedule
    general = {
        **base,
        "detailType": "movie",
        "spclbYn1": "N",
    }

    started = time.monotonic()
    try:
        r = session.post(
            SCHEDULE_API,
            data=general,
            headers=HEADERS,
            timeout=20,
        )
        print_result(
            "GENERAL POST",
            started,
            response=r,
        )
    except Exception as e:
        print_result(
            "GENERAL POST",
            started,
            error=e,
        )

    # 5) Dolby schedule
    dolby = {
        **base,
        "detailType": "spcl",
        "theabKindCd": "DBC",
        "spclbYn1": "Y",
        "theabKindCd1": "DBC",
    }

    started = time.monotonic()
    try:
        r = session.post(
            SCHEDULE_API,
            data=dolby,
            headers=HEADERS,
            timeout=20,
        )
        print_result(
            "DOLBY POST",
            started,
            response=r,
        )
    except Exception as e:
        print_result(
            "DOLBY POST",
            started,
            error=e,
        )

    print("=" * 68)
    print("DIAGNOSTIC DONE")
    print("=" * 68)


if __name__ == "__main__":
    main()
