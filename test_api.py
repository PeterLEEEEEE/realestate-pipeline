import httpx
import json

complex_no = '103577'
area_no = '1'
url = f'https://new.land.naver.com/api/complexes/{complex_no}/prices/real?tradeType=A1&areaNo={area_no}&rowCount=0'

headers = {
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://new.land.naver.com/',
}

resp = httpx.get(url, headers=headers, timeout=10.0)
data = resp.json()

if data.get('realPriceOnMonthList'):
    first_month = data['realPriceOnMonthList'][0]
    print('Month info:')
    print(f'  tradeBaseYear: {first_month.get("tradeBaseYear")}')
    print(f'  tradeBaseMonth: {first_month.get("tradeBaseMonth")}')
    print()

    if first_month.get('realPriceList'):
        item = first_month['realPriceList'][0]
        print('First real price item:')
        print(f'  formattedTradeYearMonth: {item.get("formattedTradeYearMonth")}')
        print(f'  tradeDate: {item.get("tradeDate")}')
        print(f'  dealPrice: {item.get("dealPrice")}')
        print(f'  floor: {item.get("floor")}')
        print()
        print(f'  All keys: {list(item.keys())}')
