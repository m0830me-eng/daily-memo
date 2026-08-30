import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from curl_cffi import requests

KST = ZoneInfo('Asia/Seoul')
BRANCH_NO = '1351'
SCHEDULE_API = 'https://www.megabox.co.kr/on/oh/ohc/Brch/schedulePage.do'
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Referer': 'https://www.megabox.co.kr/theater/time?brchNo=1351',
    'Origin': 'https://www.megabox.co.kr',
    'X-Requested-With': 'XMLHttpRequest',
}
OVERLOAD_TEXT = 'workload is so high'
ABORT = threading.Event()
REQUEST_GAP = 0.17
_rate_lock = threading.Lock()
_next_request_time = 0.0

def now_kst():
    return datetime.now(KST)

def dates_plus_4_to_21():
    today = now_kst().date()
    return [
        (today + timedelta(days=i)).strftime('%Y%m%d')
        for i in range(4, 22)
    ]

def params_for(date, special):
    common = {
        'masterType': 'brch',
        'brchNo': BRANCH_NO,
        'firstAt': 'N',
        'brchNo1': BRANCH_NO,
        'crtDe': now_kst().strftime('%Y%m%d'),
        'playDe': date,
    }
    if special:
        common.update({
            'detailType': 'spcl',
            'theabKindCd': 'DBC',
            'spclbYn1': 'Y',
            'theabKindCd1': 'DBC',
        })
    else:
        common.update({
            'detailType': 'movie',
            'spclbYn1': 'N',
        })
    return common

def extract_rows(data):
    mega_map = data.get('megaMap') or {}
    rows = mega_map.get('movieFormList')
    if isinstance(rows, list):
        return rows
    for value in data.values():
        if isinstance(value, dict):
            rows = value.get('movieFormList')
            if isinstance(rows, list):
                return rows
    return []

def wait_rate_slot():
    global _next_request_time
    with _rate_lock:
        now = time.monotonic()
        if now < _next_request_time:
            time.sleep(_next_request_time - now)
            now = time.monotonic()
        _next_request_time = now + REQUEST_GAP

def one_call(session, date, special):
    label = 'DOLBY' if special else 'GENERAL'
    if ABORT.is_set():
        return {'label': label, 'ok': False, 'skipped': True, 'detail': 'aborted after overload'}

    wait_rate_slot()
    started = time.monotonic()
    try:
        r = session.post(
            SCHEDULE_API,
            data=params_for(date, special),
            headers=HEADERS,
            timeout=8,
        )
    except Exception as e:
        return {
            'label': label, 'ok': False, 'skipped': False,
            'detail': f'{type(e).__name__}: {e}',
            'elapsed': time.monotonic() - started,
        }

    elapsed = time.monotonic() - started
    text_preview = (r.text or '')[:160].replace('\n', ' ').replace('\r', ' ')

    if r.status_code == 429 or OVERLOAD_TEXT in text_preview.lower():
        ABORT.set()
        return {
            'label': label, 'ok': False, 'skipped': False,
            'overload': True,
            'detail': f'HTTP={r.status_code} PREVIEW={text_preview!r}',
            'elapsed': elapsed,
        }

    if r.status_code != 200:
        return {
            'label': label, 'ok': False, 'skipped': False,
            'detail': f'HTTP={r.status_code} PREVIEW={text_preview!r}',
            'elapsed': elapsed,
        }

    try:
        data = r.json()
        rows = extract_rows(data)
        return {
            'label': label, 'ok': True, 'skipped': False,
            'rows': len(rows), 'elapsed': elapsed,
        }
    except Exception as e:
        if OVERLOAD_TEXT in text_preview.lower():
            ABORT.set()
        return {
            'label': label, 'ok': False, 'skipped': False,
            'detail': f'JSON {type(e).__name__}: {e} PREVIEW={text_preview!r}',
            'elapsed': elapsed,
        }

def test_date(date):
    session = requests.Session(impersonate='chrome')
    general = one_call(session, date, False)
    # GENERAL에서 과부하가 확인되면 이 날짜의 DOLBY는 더 때리지 않는다.
    if ABORT.is_set() and not general.get('ok'):
        dolby = {'label': 'DOLBY', 'ok': False, 'skipped': True, 'detail': 'skipped after overload'}
    else:
        dolby = one_call(session, date, True)
    return date, general, dolby

def main():
    dates = dates_plus_4_to_21()
    print('=' * 72)
    print('MEGABOX +4~+21 DAYS / 2-WORKER ONE-SHOT TEST')
    print('=' * 72)
    print('DATES:', dates[0], '~', dates[-1], f'({len(dates)} dates)')
    print('WORKERS: 2 date workers')
    print('EACH WORKER: GENERAL -> DOLBY sequentially')
    print(f'GLOBAL REQUEST START GAP: {REQUEST_GAP:.2f}s')
    print('REPEAT: NO (one shot only)')
    print('DISCORD/STATE WRITE: NO')
    print('OVERLOAD GUARD: 429 / Workload is so high -> stop extra calls')
    print('KST NOW:', now_kst().strftime('%Y-%m-%d %H:%M:%S'))
    print('=' * 72)

    started = time.monotonic()
    results = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(test_date, d) for d in dates]
        for f in as_completed(futures):
            results.append(f.result())

    results.sort(key=lambda x: x[0])
    ok_dates = 0
    failed = []
    overload = False
    for date, general, dolby in results:
        if general.get('ok') and dolby.get('ok'):
            ok_dates += 1
        else:
            failed.append(date)
        overload = overload or bool(general.get('overload')) or bool(dolby.get('overload'))
        g = 'OK' if general.get('ok') else ('SKIP' if general.get('skipped') else 'FAIL')
        d = 'OK' if dolby.get('ok') else ('SKIP' if dolby.get('skipped') else 'FAIL')
        print(f'{date} | GENERAL={g} | DOLBY={d}')
        if not general.get('ok') and not general.get('skipped'):
            print('  GENERAL:', general.get('detail', ''))
        if not dolby.get('ok') and not dolby.get('skipped'):
            print('  DOLBY:', dolby.get('detail', ''))

    elapsed = time.monotonic() - started
    print('=' * 72)
    if ok_dates == 18:
        print(f'✅ TEST RESULT: ALL 18 DATES SUCCESS | {elapsed:.2f}s')
        print('판정: +4~+21일을 2 workers + 0.17초 요청간격으로 1회 빠르게 훑는 테스트가 성공했습니다.')
    elif overload:
        print(f'❌ TEST RESULT: SERVER LIMIT / OVERLOAD DETECTED | success={ok_dates}/18 | {elapsed:.2f}s')
        print('판정: 2 workers에서도 서버 제한이 발생했습니다. 00/30 정식 적용 전 더 낮은 동시성/간격이 필요합니다.')
    else:
        print(f'⚠️ TEST RESULT: PARTIAL FAILURE | success={ok_dates}/18 | {elapsed:.2f}s')
        print('FAILED DATES:', ', '.join(failed))
    print('STATE/DISCORD CHANGED: NO')
    print('=' * 72)

if __name__ == '__main__':
    main()
