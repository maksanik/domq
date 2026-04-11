import requests

url = "https://api.cian.ru/search-offers/v2/search-offers-desktop/"

headers = {
    "accept": "*/*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type": "application/json",
    "origin": "https://www.cian.ru",
    "referer": "https://www.cian.ru/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/147.0.0.0 Safari/537.36",
}

cookies = {
    "_CIAN_GK": "cc7b701f-2b07-45a9-94a1-6e28ca2f5240",
    "cookie_agreement_accepted": "1",
    "frontend-serp.offer_chat_onboarding_shown": "1",
    "session_region_id": "1",
    "session_main_town_region_id": "1",
    "login_mro_popup": "1",
}

payload = {
    "jsonQuery": {
        "_type": "flatsale",
        "engine_version": {"type": "term", "value": 2},
        "region": {"type": "terms", "value": [1]},
        "room": {"type": "terms", "value": [1]},
        "page": {"type": "term", "value": 2},
    },
    "_liquiditySource": "web_serp",
}

response = requests.post(url, headers=headers, cookies=cookies, json=payload)

data = response.json()

print(data["data"]["offersSerialized"][0]["bargainTerms"])
