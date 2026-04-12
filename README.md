```
docker compose up -d              # PostgreSQL
python -m cian.range_scrapper     # Фаза 1: ценовые диапазоны
python -m cian.pagination_scrapper # Фаза 2: сбор объявлений → listings_raw
python -m scripts.etl_normalize   # Нормализация → buildings/flats/listings
python -m scripts.analytics       # Аналитика → price_stats/deal_analysis
uvicorn main:app --reload         # API

```
