import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина Озон

    Аргументы:
        last_id (str): Идентификатор последнего згачения из
                    предыдущего запроса.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Возвращаемое значение:
        dict | list: Ответ в формате JSON с информацией о товарах.
                    Пример ответа:
                    {
                        "items": [...],  # список товаров
                    }
                    Максимальное количество товаров ограничено значением 1000.

    Пример использования:
        >>> products = get_product_list(0, "your_clien_id", "your_seller_token"
        >>> print(products["items"][0])
        "название первого товара"

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина Озон

    Аргументы:
        clint_id (str): Идентификатор клиента.
        seller_token (str): API ключ продавца.

    Возвращаемое значение:
        list: Список артикулов (offer_ids) всех товаров магазина.

    Пример использования:
        >>> offer_ids = get_offer_ids("your_client_id", "your_seller_token")
        >>> print(offer_ids[5])
        "12345"

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров в магазине Озон через API продавца

    Аргументы:
        prices (list): Список словарей с данными о ценах товаров.
                    словарь должен содержать следующие ключи:
                    - "offer_id" (str): артикул товара
                    - "price" (str): новая цена товара
                    - "old_price" (str): старая цена товара

    Возвращаемое значение:
        dict: Ответ от API Озон в формате JSON.

    Пример использования:
        >>> prices_data = [
        ...     {"offer_id": "12345", "old_price": "9.99"},
        ...     {"offer_id": "12345", "price": "29.99"}
        ...     ]
        >>> result = update_price(
        ...     prices_data, "your_client_id", "your_seller_token"
        ... )
        >>> print(result)
        {"result": [...]}

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров в магазине Озон через API продавца

    Аргументы:
        stocks (list): Список словарей с данными об остатках товаров.
                    Словарь должен содержать следующие ключи:
                    - "offer_id" (str): Артикул товара
                    - "stock" (int): Количество товара
        client_id (str): Идентификатор клиента
        seller_token(str):API ключ продавца

    Возвращаемое значение:
        dict: Ответ от API в формате JSON

    Пример использования:
        >>> stocks_data = [
        ...     {"offer_id": "12345", "stock": 20},
        ...     {"offer_id": "98765", "stock": 10}
        ... ]
        >>> result = update_stocks(
        ...     stocks_data, "your_clien_id", "your_seller_token"
        ... )
        >>>print(result)
        {"result": [...]}

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio c остатками товаров

    Возвращаемое значение:
        dict: Список словарей с информацией об остатках товаров.
            Словарь представляет собой строку из excel файла.

    Пример использования:
        >>> stock_data = download_stock()
        >>> print(stock_data[0])
        {"Наименование": "Casio-149", "Остаток": 5}

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список товаров на основе данных с сайта и артикулов на складе

    Аргументы:
        watch_remnants (dict): Список словарей с данными об остатках товаров.
        offer_ids (list): Список артикулов товаров в магазине Озон.

    Возвращаемое значение:
        list: спиоск словарей с артикулами и количеством товара.

    Пример использования:
        >>> watch_remnants = {"Наименование": "Casio-149", "Остаток": 5}
        >>> offer_ids ={"Артикул": "12345"}
        >>> watch_stocks = create_stocks(watch_remnants, offer_ids)
        >>>print(watch_stocks)
        [{"offer_id": "Casio-149, "Остаток": 5}]

    Исключения:
        ValueError: если количество не преобразовать в число
        KeyError: если отсутствует артикул
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен на основе данных с сайта и артикулов на складе

    Аргументы:
        watch_remnants (dict): Список словарей с данными об остатках товаров.
        offer_ids (list): Список артикулов товаров в магазине Озон.

    Возвращаемое значение:
        list: список цен на артикулы.

    Пример использования:
        >>> watch_remnants = {"Наименование": "Casio-149", "Цена": "5'990.00 руб"}
        >>> offer_ids ={"Артикул": "12345"}
        >>> watch_prices = create_prices(watch_remnants, offer_ids)
        >>>print(watch_prices)
        [{"offer_id": "12345", "Цена": "5'990.00 руб"}]

    Исключения:
        ValueError: если количество не преобразовать в число
        KeyError: если отсутствует артикул
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену

    Аргумент:
        price (str): Цена в исходном формате

    Возвращаемое значение:
        str: Цена в числовом формате

    Пример использования: 
        >>> price_conversion("5'990.00 руб.")
        5990
    
    Исключения:
        AttributeError: если price не строка
        IndexError: если строка не содержит точку
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Аргументы:
        lst (list): исходный список
        n (int): размер части

    Возвращаемое значение:
        list: часть списка размером n элементов

    Пример использования:
        >>> List(divide([1,2,3,4,5], 2))
        [[1,2],[3,4],[5]]

    Исключения:
        TypeError: если n не целое число
        ValueError: если nменьше или равно 0
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновить цены товаров

    Аргументы:
        watch_remnants (list): Список остатков товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен доступа продавца

    Возвращаемое значение:
        list: Список обновленных цен

    Пример использования:
        >>> remnants = [{"Наименование"}: "Casio-149", "Цена": 5'990.00 руб"}]
        >>> prices = asyncio.run(
        ...     upload_prices(
        ...         remnants, "your_client_id", "your_seller_token"
        ...     )
        ... )
        >>> print(len(prices))
        1

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновить остатки товаров

    Аргументы:
        watch_remnants (list): Список остатков товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен доступа продавца

    Возвращаемое значение:
        tuple: кортеж с непустыми остатками и всеми остатками

    Пример использования:
        >>> remnants = [{"Наименование"}: "Casio-149", "Остатки": "5"}]
        >>> not empty, all_stocks = asyncio.run(
        ...     upload_stocks(
        ...         remnants, "your_client_id", "your_seller_token"
        ...     )
        ... )
        >>> print(len(not_empty))
        1

    Исключения:
        requests.exceptions.HTTPError
        requests.exceptions.RequestsException
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
