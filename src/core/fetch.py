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


def fetch_articles_paged(
    client: httpx.Client,
    complex_id: int | str,
    trade_type: str = "A1",
    max_pages: int = 3, # 페이지 당 20개
    sleep_ms_min: int = 200,
    sleep_ms_max: int = 500,
) -> list[dict]:
    """
    단일 complex에 대해 최대 max_pages까지 페이지를 호출하여 기사 배열을 반환.
    응답의 isMoreData가 False면 조기 종료. 페이지 간 랜덤 sleep 포함.
    """
    all_articles: list[dict] = []
    for page_no in range(1, max_pages + 1):
        url = f"{BASE_URL + complex_articles_url.format(complex_no=complex_id, trade_type=trade_type, page_no=page_no, order_type='prc')}"
        try:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articleList", [])
        except httpx.HTTPError:
            break

        if not articles:
            break

        all_articles.extend(articles)

        if data.get("isMoreData") is False:
            break

        time.sleep(random.randint(sleep_ms_min, sleep_ms_max) / 1000.0)

    return all_articles

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
    sleep_min_sec: int = 1,
    sleep_max_sec: int = 3
) -> list[dict]:
    """
    단일 complex의 실거래가 정보를 반환.
    """

    if pyeong_cnt <= 0:
        return []
    
    result = []
    
    for pyeong_dict in pyeong_infos:
        area_no = pyeong_dict.get("pyeongNo", 1)
        row_count = 0  # 각 평형마다 0부터 시작
        current_year = '2025'
        while True:
            url = f"{BASE_URL + complex_real_price_url.format(complex_no=complex_no, trade_type=trade_type, area_no=area_no, row_count=row_count)}"

            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                added_row_count = data.get("addedRowCount", 0)
                total_row_count = data.get("totalRowCount", 0)

                sold_month_list = data.get('realPriceOnMonthList', [])

                if sold_month_list:
                    for sold_month_info in sold_month_list:
                        trade_base_year = sold_month_info.get("tradeBaseYear", '0')
                        if trade_base_year == '2025':

                            for sold_info in sold_month_info.get("realPriceList", []):
                                if sold_info.get("deleteYn"):
                                    continue

                                sold_info['complexNo'] = complex_no
                                sold_info['areaNo'] = area_no
                                sold_info['pyeong'] = pyeong_dict.get("pyeongName2", "")
                                result.append(sold_info)
                        else:
                            # 2025년이 아니면 이 평형의 데이터 수집 종료
                            current_year = trade_base_year
                            break

                # 모든 데이터를 가져왔으면 다음 평형으로
                if added_row_count >= total_row_count or current_year != '2025':
                    break

                # 다음 페이지를 위해 row_count 업데이트
                row_count = added_row_count
                time.sleep(random.randint(sleep_min_sec, sleep_max_sec))

            except httpx.HTTPError:
                break  # 이 평형은 에러로 종료, 다음 평형으로
        
    
    
    return result
