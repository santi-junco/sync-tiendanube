import requests

from app.logger import logger


class Tiendanube():

    def get_products(self, url: str, headers: dict, params: dict):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f"Error fetching products from Tiendanube: {response.status_code} - {response.text}")
            return []
        logger.info(f"Fetched {len(response.json())} products from Tiendanube")
        return response.json()

    def update_stock(self, url: str, headers: dict, data: dict):
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            logger.error(f"Error updating stock in Tiendanube: {response.status_code} - {response.text}")
            return {}
        logger.info("Stock updated in Tiendanube")
        return response.json()

    def get_categories(self, url: str, headers: dict, params: dict):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f"Error fetching categories from Tiendanube: {response.status_code} - {response.text}")
            return []
        logger.info(f"Fetched {len(response.json())} categories from Tiendanube")
        return response.json()
