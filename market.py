"""Something about Yandex market?"""
import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из кампании на Яндекс маркете.

    Аргументы:
        page (str): Идентификатор последнего значения из
                предыдущего запроса.
        campaign_id (str): Идентификатор кампании.
        Access_token (str): API токен доступа.

    Возвращаемое значение:
        dict: Ответ API с данными о товарах.

    Пример использования:
        >>> get_product_list("", "your_campaign_id, "your_access_token")
        {"result": {"offerMappingEntries": [...]}, "paging": {...}}

    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        Exception: при остальных ошибках.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров для кампании.

    Аргументы:
        stocks (list): Список данных об остатках.
        campaign_id (str): Идентификатор кампании.
        access_token (str): API токен доступа.

    Возвращаемое значение:
        dict: Ответ API после обновления остатков.

    Пример использования:
        >>> stocks = [{"sku": "123", "warehouseId": "456", "items": [...]}]
        >>> update_stocks(stocks, "your_campaign_id", "your_access_token")
        {"status": "OK"}

    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        Exception: при остальных ошибках.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров для компании.

    Аргументы:
        prices (list): Список данных об ценах.
        campaign_id (str): Идентификатор кампании.
        access_token (str): API токен доступа.

    Возвращаемое значение:
        dict: Ответ API после обновления цен.

    Пример использования:
        >>> prices = [
                {"id": "123", "price": {"value": 5990, "currencyId": "RUR"}}
            ]
        >>> update_prices(prices, "your_campaign_id", "your_access_token")
        {"status": "OK"}

    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        Exception: при остальных ошибках.

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы из кампании на Яндекс маркете.

    Аргументы:
        campaign_id (str): Идентификатор кампании.
        market_token (str): API токен доступа.

    Возвращаемое значение:
        list: Список артикулов товаров.

    Пример использования:
        >>> get_offer_ids("your_campaign_id", "your_market_token")
        ["12345", "67890"]

    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        Exception: при остальных ошибках.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать данные об остатках товаров на складе Яндекса.

    Аргументы:
        watch_remnants (list): Данные об остатках.
        offer_ids (list): Список артикулов товаров.
        warehouse_id (str): Идентификатор склада.

    Возвращаемое значение:
        list: Список данных для обновления остатков.

    Пример использования:
        >>> watch_remanants = [{"Код"; "123", "Остатки": "10"}, ...]
        >>> offer_ids = ["123", "456"]
        >>> create_stocks(watch_remnants, offer_ids, "your_warehouse_id")
        [{"sku": "123", "warehouseId": "your_warehouse_id", "items": [...]}]

    Исключения:
        KeyError: если в данных нет ключей.
        ValueError: если нельзя преобразовать количество в числовой формат.
        TypeError: неверный тип данных.
        Exception: при остальных ошибках.
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(
        datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        )
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены для обновления.

    Аргументы:
        watch_remnants (list): Данные об остатках.
        offer_ids (list): Список артикулов товаров.

    Возвращаемое значение:
        list: Список данных для обновления цен.

    Пример использования:
        >>> watch_remanants = [{"Код"; "123", "Остатки": "10"}, ...]
        >>> offer_ids = ["123", "456"]
        >>> create_prices(watch_remnants, offer_ids)
        [{"id": "123", "price": {"value": 5990, "currencyId": "RUR"}}]

    Исключения:
        KeyError: если в данных нет ключей.
        ValueError: если нельзя преобразовать количество в числовой формат.
        TypeError: неверный тип данных.
        Exception: при остальных ошибках.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновить цены товаров в кампании.

    Аргументы:
        watch_remnants (list): Данные об остатках.
        campaign_id (list): Идентификатор кампании.
        market_token (str): API токен доступа.

    Возвращаемое значение:
        list: Список обновленных цен.

    Пример использования:
        >>> watch_remanants = [{"Код"; "123", "Цена": "10"}, ...]
        >>> asyncio.run(
                upload_prices(
                    watch_remnants, "your_campaign_id", "your_market_token"
                )
            )
        [{"id": "123", "price": {"value": {...}}]

    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        ValueError: если нельзя преобразовать количество в числовой формат.
        Exception: при остальных ошибках.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(
                    watch_remnants,
                    campaign_id,
                    market_token,
                    warehouse_id
        ):
    """Асинхронно обновить остатки товаров в кампании.

    Аргументы:
        watch_remnants (list): Данные об остатках.
        campaign_id (list): Идентификатор кампании.
        market_token (str): API токен доступа.
        warehouse_id (str): Идентификатор склада.

    Возвращаемое значение:
        tuple: кортеж с непустыми остатками и всеми остатками

    Пример использования:
        >>> watch_remanants = [{"Код"; "123", "Цена": "10"}, ...]
        >>> asyncio.run(
                upload_prices(
                    watch_remnants,
                    "your_campaign_id",
                    "your_market_token",
                    "your_warehouse_id"
                )
            )
        ([...],[...])


    Исключения:
        requests.exceptions.HTTPError: ответ от API с ошибкой.
        requests.exceptions.ConnectionError: ошибка соединения.
        KeyError: если в данных нет ключей.
        ValueError: если нельзя преобразовать количество в числовой формат.
        Exception: при остальных ошибках.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Docstring?"""
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
