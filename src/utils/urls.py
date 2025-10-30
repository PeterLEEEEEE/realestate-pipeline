BASE_URL = "https://new.land.naver.com/api"

# 지역구 별 아파트 명 API
complex_name_url = "/search?keyword={region_name}&page={page_no}"

# 아파트 메타데이터(detail) API
complex_metadata_url = "/complexes/overview/{complex_no}"

# 아파트의 매물 리스트 API
complex_articles_url = "/articles/complex/{complex_no}?tradeType={trade_type}&page={page_no}&sameAddressGroup=True&type=list&order={order_type}"

# 아파트 매물 상세 API
complex_article_detail_url = "/articles/{article_no}"

# 아파트 상하한가 API
complex_limit_price_url = "/complexes/{complex_no}/prices?complexNo={complex_no}&tradeType={trade_type}&year={year}&priceChartChange=true&areaNo={area_no}&areaChange=true&type=table"

# 아파트 실거래가 API(중요)
complex_real_price_url = "/complexes/{complex_no}/prices/real?complexNo={complex_no}&tradeType={trade_type}&year=5&priceChartChange=true&areaNo={area_no}&addedRowCount={row_count}&type=table"

# 평 타입
complex_area_types_url = "/complexes/{complex_no}/buildings/pyeongtype?dongNo={dong_no}&complexNo={complex_no}"
""" 데이터 예시
{
    "dongNo": "864249",
    "hoListOnFloor": [
        {
            "floor": 27,
            "pyeongHoList": [
                {
                    "hoNo": "69",
                    "hoName": "2701",
                    "hoFloor": 27,
                    "pyeongNo": "2",
                    "pyeongContent": "공급 110.54",
                    "supplyArea": "110.54",
                    "totalArea": "110.54",
                    "pyeongClassString": "8_1",
                    "lineNo": "1",
                    "pilotiYn": "N",
                    "existHo": "Y",
                    "pyeongName": "110",
                    "pyeongNameDecimal": "110.54"
                },
                {
                    "hoNo": "70",
                    "hoName": "2702",
                    "hoFloor": 27,
                    "pyeongNo": "2",
                    "pyeongContent": "공급 110.54",
                    "supplyArea": "110.54",
                    "totalArea": "110.54",
                    "pyeongClassString": "8_1",
                    "lineNo": "2",
                    "pilotiYn": "N",
                    "existHo": "Y",
                    "pyeongName": "110",
                    "pyeongNameDecimal": "110.54"
                },
                {
                    "hoNo": "71",
                    "hoName": "2703",
                    "hoFloor": 27,
                    "pyeongNo": "2",
                    "pyeongContent": "공급 110.54",
                    "supplyArea": "110.54",
                    "totalArea": "110.54",
                    "pyeongClassString": "8_1",
                    "lineNo": "3",
                    "pilotiYn": "N",
                    "existHo": "Y",
                    "pyeongName": "110",
                    "pyeongNameDecimal": "110.54"
                },
                {
                    "hoNo": "72",
                    "hoName": "2704",
                    "hoFloor": 27,
                    "pyeongNo": "2",
                    "pyeongContent": "공급 110.54",
                    "supplyArea": "110.54",
                    "totalArea": "110.54",
                    "pyeongClassString": "8_1",
                    "lineNo": "4",
                    "pilotiYn": "N",
                    "existHo": "Y",
                    "pyeongName": "110",
                    "pyeongNameDecimal": "110.54"
                }
            ]
        },
"""


# 특정 매물 시세 변화 API
article_price_history_url = "/article-price-history/{article_no}"
""" 데이터 예시
{
    "tradeType": "A1",
    "initialPrice": "29억 2,000",
    "priceHistoryList": [
        {
            "modificationYearMonthDay": "20250930",
            "priceState": "INCREASE",
            "priceGap": 3000
        }
    ]
}

{
"tradeType": "B2",
"initialPrice": "10억 5,000/60",
"priceHistoryList": [
    {
        "modificationYearMonthDay": "20250929",
        "priceState": "INCREASE",
        "priceGap": 5000,
        "rentPriceState": "DECREASE",
        "rentPriceGap": 20
    }
]
}
"""