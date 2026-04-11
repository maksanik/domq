import re
from bs4 import BeautifulSoup
import json

# читаем локальный файл
with open("cian/tmp.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

scripts = soup.find_all("script")

products: list = None  # type: ignore

for script in scripts:
    if script.string and "var i" in script.string and "products" in script.string:
        text = script.string

        # вытаскиваем объект var i = {...}
        match = re.search(r"var i\s*=\s*(\{.*\})\s*;", text, re.S)
        if not match:
            continue

        obj_text = match.group(1)

        try:
            data = json.loads(obj_text)
            products = data.get("products")
            break
        except Exception:
            continue

print(products, len(products), sep="\n")

for i in range(len(products)):
    print(products[i]["price"])
