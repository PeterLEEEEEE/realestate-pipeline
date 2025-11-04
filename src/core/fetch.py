from json.encoder import py_encode_basestring_ascii
import time
import random
import httpx
import asyncio

from datetime import datetime
from src.utils.urls import (
    BASE_URL,
    complex_metadata_url,
    article_price_history_url,
    complex_limit_price_url,
    complex_real_price_url,
    complex_name_url,
    complex_articles_url
)
from src.models.model import RealEstateComplex
from src.utils.headers import get_cookies_headers

async def process_mongo(data_list: list[dict], mongo_service):

    docs = []
    now = datetime.now()
    for data in data_list:
        try:
            if data.get("realEstateTypeCode") in ["APT", "JGC", "ABYG"]:
                model = RealEstateComplex(**data)
                doc = model.model_dump(by_alias=True, exclude_unset=True)
                doc["crawled_at"] = now
                doc["low_floor"] = data.get("lowFloor", 0)
                doc["high_floor"] = data.get("highFloor", 0)
                docs.append(doc)
        except Exception as e:
            print(f"데이터 처리 오류: {e}")

    if docs:
        await mongo_service.upsert_many(docs)
        print(f"[Mongo] {len(docs)}건 upsert 완료")

        
async def fetch_complex(region: str, client: httpx.AsyncClient,
                        sem: asyncio.Semaphore, mongo_service):

    page_no = 1
    total_processed = 0
    while True:
        url = f"{BASE_URL + complex_name_url.format(region_name=region, page_no=page_no)}"
        print(f"[{region}] Fetching page {page_no}: {url}")
        
        async with sem:
            
            print(f"[{region}] GET {url}")
            try:
                response = await client.get(url, timeout=15.0)
                response.raise_for_status()
            
                data = response.json()
                complexes = data.get("complexes", [])
                
                if not complexes:
                    print(f"[{region}] page {page_no}에 아파트 단지 정보 없음 → 종료")
                    break
                
                await process_mongo(complexes, mongo_service)
                total_processed += len(complexes)
                print(f"[{region}] page {page_no}: 누적 처리 {total_processed}건 (+{len(complexes)})")
            
            except httpx.HTTPStatusError as e:
                print(f"[{region}] HTTP 오류 {e.response.status_code}: {e.request.url}")
                return
            except httpx.RequestError as e:
                print(f"[{region}] 네트워크 오류: {e}")
                return
            
            # ---- 응답 처리 ----
            if data.get("isMoreData") is False:   # 더 이상 페이지 없음
                print(f"[{region}] 모든 페이지 크롤링 완료 (총 {total_processed}건)")
                break
            
        await asyncio.sleep(random.randint(5, 20))
        page_no += 1
    

async def fetch_complex_price_async(complex_ids):
    """
    아파트 단지의 가격 정보를 가져오는 함수
    """
    url = f"{BASE_URL + complex_metadata_url.format(complex_no=complex_ids[0])}"
    print(f"GET {url}")
    
    cookies, headers = get_cookies_headers()
    async with httpx.AsyncClient(headers=headers, cookies=cookies, timeout=15.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data
        except httpx.HTTPStatusError as e:
            print(f"HTTP 오류 {e.response.status_code}: {e.request.url}")
        except httpx.RequestError as e:
            print(f"네트워크 오류: {e}")


def fetch_complex_price(complex_ids):
    url = f"{BASE_URL + complex_metadata_url.format(complex_no=236)}"
    print(f"GET {url}")

    cookies, headers = get_cookies_headers()
    with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
        try:
            response = client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP 오류 {e.response.status_code}: {e.request.url}")
        except httpx.RequestError as e:
            print(f"네트워크 오류: {e}")
    
    return {}
    

async def fetch_articles_async(complex_id=236):
    # A1 매매, B1 전세
    
    page_no = 1
    url = f"{BASE_URL + complex_articles_url.format(complex_no=complex_id, trad_type="A1", page_no=page_no, order_type="dateDesc")}"
    print(f"GET {url}")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=15.0)
            response.raise_for_status()
            data = response.json()
            articles = data.get("articles", [])

            # for article in articles:
                # 기사 데이터 처리 로직 추가
                # ...
            return articles

        except httpx.HTTPStatusError as e:
            print(f"HTTP 오류 {e.response.status_code}: {e.request.url}")
        except httpx.RequestError as e:
            print(f"네트워크 오류: {e}")

    return []

def fetch_articles(complex_id=236):
    # A1 매매, B1 전세
    
    page_no = 1
    cookies, headers = get_cookies_headers()
    articles = []
    
    while page_no <= 5:
        url = f"{BASE_URL + complex_articles_url.format(complex_no=complex_id, trade_type="A1", page_no=page_no, order_type="dateDesc")}"
        print(f"GET {url}")
        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            try:
                response = client.get(url)
                response.raise_for_status()
                data = response.json()
                articles.append(data.get("articleList", []))
                
                if data.get("isMoreData") is False or page_no >= 5:
                    print(f"모든 페이지 크롤링 완료 (총 {len(articles)}건)")
                    break
                # for article in articles:
                    # 기사 데이터 처리 로직 추가
                    # ...
            except httpx.HTTPStatusError as e:
                print(f"HTTP 오류 {e.response.status_code}: {e.request.url}")
            except httpx.RequestError as e:
                print(f"네트워크 오류: {e}")

    return articles


def fetch_articles_for_areas(
    client: httpx.Client,
    complex_id: int | str,
    area_nos: list[int],
    trade_type: str = "A1",
    max_pages: int = 3, # 지금은 안쓰긴 하는데 rate limit 때문에 일단 남겨둠
    sleep_ms_min: int = 10,
    sleep_ms_max: int = 15,
) -> list[dict]:
    """
    단일 complex의 특정 평형들(area_nos)에 대해 매물 리스트 수집.

    Args:
        client: HTTP 클라이언트
        complex_id: 단지 ID
        area_nos: 평형 번호 리스트 (예: [1, 2, 3, 4, 5])
        trade_type: 거래 타입 (A1: 매매, B1: 전월세)
        max_pages: 최대 페이지 수
        sleep_ms_min: 최소 sleep 시간 (ms)
        sleep_ms_max: 최대 sleep 시간 (ms)

    Returns:
        매물 리스트 (가격 히스토리 없음, 원본 데이터)
    """
    articles = []
    area_nos_str = ":".join(map(str, area_nos))
    page_no = 1

    while True:
        url = f"{BASE_URL + complex_articles_url.format(complex_no=complex_id, trade_type=trade_type, area_nos=area_nos_str, page_no=page_no, order_type='prc')}"
        try:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
            article_list = data.get("articleList", [])
        except httpx.HTTPError:
            break

        if not article_list:
            break

        articles.extend(article_list)

        if data.get("isMoreData") is False:
            break

        page_no += 1
        time.sleep(random.randint(sleep_ms_min, sleep_ms_max))

    return articles

def fetch_article_price_history(
    client: httpx.Client,
    article_no: int | str,
) -> list[dict]:
    """
    단일 article의 가격 변동 이력을 반환.
    """

    url = f"{BASE_URL + article_price_history_url.format(article_no=article_no)}"
    try:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError:
        return []


def enrich_articles_with_price_history(
    client: httpx.Client,
    articles: list[dict],
    complex_id: int | str,
) -> list[dict]:
    """
    매물 리스트에 가격 히스토리를 추가하고 날짜 필드를 변환.

    Args:
        client: HTTP 클라이언트
        articles: 매물 리스트 (fetch_articles_paged 결과)
        complex_id: 단지 ID

    Returns:
        가격 히스토리와 날짜 변환이 완료된 매물 리스트
    """
    from datetime import timedelta

    enriched = []

    for article in articles:
        article_no = article.get("articleNo")
        if article_no is None:
            continue

        # 가격 변동이 있으면 히스토리 API 호출
        initial_price = None
        price_history_list = []
        if article.get("priceChangeState") != "SAME":
            history = fetch_article_price_history(
                client=client,
                article_no=article_no,
            )
            if isinstance(history, dict):
                initial_price = history.get("initialPrice")
                price_history_list = history.get("priceHistoryList") or []

            time.sleep(random.randint(2, 5))
        
        # payload 구성
        payload = {**article}
        if initial_price is not None:
            payload["initialPrice"] = initial_price
        if price_history_list:
            payload["priceHistoryList"] = price_history_list
        if not payload.get("complexNo"):
            payload["complexNo"] = str(complex_id)

        # articleConfirmYmd를 datetime으로 변환
        ymd = payload.get("articleConfirmYmd")
        if isinstance(ymd, str) and len(ymd) == 8 and ymd.isdigit():
            try:
                confirm_dt = datetime.strptime(ymd, "%Y%m%d")
                payload["articleConfirmDate"] = confirm_dt
                payload["expireAt"] = confirm_dt + timedelta(days=28)
            except Exception:
                pass

        enriched.append(payload)

    return enriched


def fetch_complex_detail(
    client: httpx.Client,
    complex_id: int | str,
    sleep_min_sec: int,
    sleep_max_sec: int
) -> dict:
    """
    단일 complex의 상세 정보를 반환.
    """

    url = f"{BASE_URL + complex_metadata_url.format(complex_no=complex_id)}"
    
    try:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()

        time.sleep(random.randint(sleep_min_sec, sleep_max_sec))
        return data
    except httpx.HTTPError:
        return {}

def fetch_complex_real_price(
    client: httpx.Client,
    complex_no: int | str,
    pyeong_infos: list[dict],
    pyeong_cnt: int,
    trade_type: str = "A1",
    sleep_min_sec: int = 2,
    sleep_max_sec: int = 5
) -> list[dict]:
    """
    단일 complex의 실거래가 정보를 2023년부터 현재까지 전체 수집.
    초기 데이터 수집용.
    """

    if pyeong_cnt <= 0:
        return []

    result = []
    min_year = 2023  # 2023년부터 수집

    for pyeong_dict in pyeong_infos:
        area_no = pyeong_dict.get("pyeongNo", 1)
        row_count = 0  # 각 평형마다 0부터 시작

        while True:
            url = f"{BASE_URL + complex_real_price_url.format(complex_no=complex_no, trade_type=trade_type, area_no=area_no, row_count=row_count)}"

            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                added_row_count = data.get("addedRowCount", 0)
                total_row_count = data.get("totalRowCount", 0)

                sold_month_list = data.get('realPriceOnMonthList', [])
                if not sold_month_list:
                    break

                stop_collection = False
                for sold_month_info in sold_month_list:
                    trade_base_year = sold_month_info.get("tradeBaseYear", '0')

                    try:
                        year_int = int(trade_base_year)
                    except (ValueError, TypeError):
                        continue

                    # 2023년 이상인 경우만 수집
                    if year_int >= min_year:
                        for sold_info in sold_month_info.get("realPriceList", []):
                            if sold_info.get("deleteYn"):
                                continue

                            sold_info['complexNo'] = complex_no
                            sold_info['areaNo'] = area_no
                            sold_info['pyeong'] = pyeong_dict.get("pyeongName2", "")
                            result.append(sold_info)
                    elif year_int < min_year:
                        # 2023년 이전 데이터 발견 시 수집 종료
                        stop_collection = True
                        break

                # 종료 조건
                if stop_collection or added_row_count >= total_row_count:
                    break

                # 다음 페이지를 위해 row_count 업데이트
                row_count = added_row_count
                time.sleep(random.randint(sleep_min_sec, sleep_max_sec))

            except httpx.HTTPError:
                break  # 이 평형은 에러로 종료, 다음 평형으로

    return result

# 로직 설계 상 안씀
def fetch_complex_real_price_current_month(
    client: httpx.Client,
    complex_no: int | str,
    pyeong_infos: list[dict],
    pyeong_cnt: int,
    target_year: str,
    target_month: str,
    target_day: int,
    trade_type: str = "A1",
    sleep_min_sec: int = 2,
    sleep_max_sec: int = 5
) -> list[dict]:
    """
    단일 complex의 실거래가 정보를 현재 연월일 기준으로 필터링하여 반환.

    Args:
        client: HTTP 클라이언트
        complex_no: 단지 번호
        pyeong_infos: 평형 정보 리스트
        pyeong_cnt: 평형 개수
        target_year: 수집 대상 년도 (예: "2025")
        target_month: 수집 대상 월 (예: "11")
        target_day: 수집 대상 일 (이 날짜까지의 데이터만 수집, 예: 3)
        trade_type: 거래 타입 (A1: 매매)
        sleep_min_sec: 최소 sleep 시간 (초)
        sleep_max_sec: 최대 sleep 시간 (초)

    Returns:
        필터링된 실거래가 리스트

    Example:
        # 2025년 11월 3일까지의 데이터만 수집
        fetch_complex_real_price_current_month(
            client, complex_no, pyeong_infos, pyeong_cnt,
            target_year="2025",
            target_month="11",
            target_day=3
        )
    """

    if pyeong_cnt <= 0:
        return []

    result = []

    for pyeong_dict in pyeong_infos:
        area_no = pyeong_dict.get("pyeongNo", 1)
        row_count = 0  # 각 평형마다 0부터 시작

        while True:
            url = f"{BASE_URL + complex_real_price_url.format(complex_no=complex_no, trade_type=trade_type, area_no=area_no, row_count=row_count)}"

            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                added_row_count = data.get("addedRowCount", 0)
                total_row_count = data.get("totalRowCount", 0)

                sold_month_list = data.get('realPriceOnMonthList', [])
                if not sold_month_list:
                    break

                found_target_month = False
                found_other_month_after_target = False

                for sold_month_info in sold_month_list:
                    trade_base_year = sold_month_info.get("tradeBaseYear", '0')
                    trade_base_month = sold_month_info.get("tradeBaseMonth", '0')

                    # 대상 연, 월과 일치하는 경우만 처리
                    if trade_base_year == target_year and trade_base_month == target_month:
                        found_target_month = True

                        for sold_info in sold_month_info.get("realPriceList", []):
                            if sold_info.get("deleteYn"): # 계약 취소된 매물은 무시
                                continue

                            # tradeDate가 target_day 이하인 경우만 수집
                            trade_date_str = sold_info.get("tradeDate", "0")
                            try:
                                trade_date = int(trade_date_str)
                            except (ValueError, TypeError):
                                continue

                            if trade_date == target_day: # 연, 월, 일이 모두 같은 경우만 저장
                                sold_info['complexNo'] = complex_no
                                sold_info['areaNo'] = area_no
                                sold_info['pyeong'] = pyeong_dict.get("pyeongName2", "")
                                result.append(sold_info)
                    elif found_target_month:
                        # 대상 월을 이미 찾았는데 다른 월이 나왔다면
                        # 더 이상 대상 월 데이터가 없으므로 이 평형 수집 종료
                        found_other_month_after_target = True
                        break

                # 대상 월 이후 다른 월이 나왔거나, 모든 데이터를 가져왔으면 다음 평형으로
                if found_other_month_after_target or added_row_count >= total_row_count:
                    break

                # 다음 페이지를 위해 row_count 업데이트
                row_count = added_row_count
                time.sleep(random.randint(sleep_min_sec, sleep_max_sec))

            except httpx.HTTPError:
                break  # 이 평형은 에러로 종료, 다음 평형으로

    return result


def fetch_complex_real_price_recent(
    client: httpx.Client,
    complex_no: int | str,
    pyeong_infos: list[dict],
    pyeong_cnt: int,
    execution_year: str,
    execution_month: str,
    execution_day: int,
    trade_type: str = "A1",
    sleep_min_sec: int = 2,
    sleep_max_sec: int = 5
) -> list[dict]:
    """
    단일 complex의 실거래가 정보를 지난 달 1일부터 실행일까지 수집 (증분 수집용).

    지난 달 전체 + 현재 달 (1일~실행일) 데이터를 수집합니다.

    Args:
        client: HTTP 클라이언트
        complex_no: 단지 번호
        pyeong_infos: 평형 정보 리스트
        pyeong_cnt: 평형 개수
        execution_year: 실행 년도 (예: "2025")
        execution_month: 실행 월 (예: "11")
        execution_day: 실행 일 (예: 3)
        trade_type: 거래 타입 (A1: 매매)
        sleep_min_sec: 최소 sleep 시간 (초)
        sleep_max_sec: 최대 sleep 시간 (초)

    Returns:
        지난 달 1일부터 실행일까지의 실거래가 리스트

    Example:
        # 실행일이 2025년 11월 3일인 경우
        # 2025년 10월 1일 ~ 2025년 11월 3일 데이터 수집
        fetch_complex_real_price_recent(
            client, complex_no, pyeong_infos, pyeong_cnt,
            execution_year="2025", execution_month="11", execution_day=3
        )
    """
    from datetime import date, timedelta

    if pyeong_cnt <= 0:
        return []

    # 실행일 생성
    exec_date = date(int(execution_year), int(execution_month), execution_day)

    # 지난 달 1일 계산
    first_of_current_month = exec_date.replace(day=1)
    last_month_last_day = first_of_current_month - timedelta(days=1)
    first_of_last_month = last_month_last_day.replace(day=1)

    # 날짜 범위 설정: 지난 달 1일 ~ 실행일
    start_date = first_of_last_month
    end_date = exec_date

    result = []

    for pyeong_dict in pyeong_infos:
        area_no = pyeong_dict.get("pyeongNo", 1)
        row_count = 0

        while True:
            url = f"{BASE_URL + complex_real_price_url.format(complex_no=complex_no, trade_type=trade_type, area_no=area_no, row_count=row_count)}"

            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                added_row_count = data.get("addedRowCount", 0)
                total_row_count = data.get("totalRowCount", 0)

                sold_month_list = data.get('realPriceOnMonthList', [])
                if not sold_month_list:
                    break

                stop_collection = False

                for sold_month_info in sold_month_list:
                    trade_base_year = sold_month_info.get("tradeBaseYear", '0')
                    trade_base_month = sold_month_info.get("tradeBaseMonth", '0')

                    try:
                        year = int(trade_base_year)
                        month = int(trade_base_month)
                    except (ValueError, TypeError):
                        continue

                    # 월 기준으로 빠른 필터링 (범위 밖이면 스킵/종료)
                    month_first_day = date(year, month, 1)

                    # 범위보다 미래 월 → 스킵
                    if month_first_day > end_date:
                        continue

                    # 범위보다 과거 월 → 종료
                    if month_first_day < start_date.replace(day=1):
                        stop_collection = True
                        break

                    # 범위 내 월 → 데이터 수집
                    for sold_info in sold_month_info.get("realPriceList", []):
                        if sold_info.get("deleteYn"):
                            continue

                        trade_date_str = sold_info.get("tradeDate", "0")
                        try:
                            day = int(trade_date_str)
                            trade_date = date(year, month, day)
                        except (ValueError, TypeError):
                            continue

                        # 범위 체크
                        if start_date <= trade_date <= end_date:
                            sold_info['complexNo'] = complex_no
                            sold_info['areaNo'] = area_no
                            sold_info['pyeong'] = pyeong_dict.get("pyeongName2", "")
                            
                            result.append(sold_info)

                # 종료 조건
                if stop_collection or added_row_count >= total_row_count:
                    break

                # 다음 페이지
                row_count = added_row_count
                time.sleep(random.randint(sleep_min_sec, sleep_max_sec))

            except httpx.HTTPError:
                break

    return result
