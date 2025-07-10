import requests
from datetime import datetime

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

    def fetch_recent_variants(self, tienda_config, updated_at_min):
        url = f"{tienda_config['url']}/products"
        headers = tienda_config['headers']
        page = 1
        products = []
        variants = []

        while True:
            params = {
                "per_page": 200,
                "published": "true",
                "fields": "variants",
                "page": page,
                "updated_at_min": updated_at_min
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                products.extend(data)

                total_prod = int(response.headers.get("x-total-count", 0))

                if len(products) >= total_prod:
                    break

                page += 1
            else:
                logger.error(f"Error fetching products from Tiendanube: {response.status_code} - {response.text}")
                return []

        for product in products:
            for variant in product.get("variants", []):
                variant_date = datetime.strptime(variant["updated_at"], "%Y-%m-%dT%H:%M:%S%z").isoformat()
                if variant_date >= updated_at_min:
                    variants.append(variant)

        return variants
