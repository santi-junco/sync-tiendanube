import os
import requests

from app.logger import logger


class Shopify():
    def __init__(self):
        self.SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
        self.SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
        self.SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION")
        self.SHOPIFY_API_URL = f"{self.SHOPIFY_STORE_URL}/admin/api/{self.SHOPIFY_API_VERSION}"
        self.DEFAULT_DEPOSIT = "104501772590"
        self.SHOPIFY_HEADERS = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.SHOPIFY_ACCESS_TOKEN
        }

    def get_products(self, params: dict = {}):
        response = requests.get(f"{self.SHOPIFY_API_URL}/products.json", params=params, headers=self.SHOPIFY_HEADERS)
        if response.status_code != 200:
            logger.error(f"Error fetching products from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Fetched {len(response.json())} products from Shopify")
        return response.json()

    def get_product(self, product_id: int, params: dict = {}):
        response = requests.get(f"{self.SHOPIFY_API_URL}/products/{product_id}.json", params=params, headers=self.SHOPIFY_HEADERS)
        if response.status_code != 200:
            logger.error(f"Error fetching product from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Fetched product {product_id} from Shopify")
        return response.json()

    def create_product(self, data: dict):
        response = requests.post(f"{self.SHOPIFY_API_URL}/products.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 201:
            logger.error(f"Error creating product in Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Created product {data['product']['title']} in Shopify")
        return response.json()

    def update_product(self, product_id: int, data: dict):
        response = requests.put(f"{self.SHOPIFY_API_URL}/products/{product_id}.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 200:
            logger.error(f"Error updating product in Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Updated product {product_id} in Shopify")
        return response.json()

    def set_inventory_level(self, data: dict):
        response = requests.post(f"{self.SHOPIFY_API_URL}/inventory_levels/set.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 200:
            logger.error(f"Error setting inventory level in Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info("Set inventory level in Shopify")
        return response.json()

    def set_default_inventory_level(self, inventory_item_id: int, data: dict = None):
        if data is None:
            data = {
                "location_id": self.DEFAULT_DEPOSIT,
                "inventory_item_id": inventory_item_id,
                "available": 0
            }
        response = requests.post(f"{self.SHOPIFY_API_URL}/inventory_levels/set_default.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 200:
            logger.error(f"Error setting default inventory level in Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info("Set default inventory level in Shopify")
        return response.json()

    def get_product_images(self, product_id: int):
        response = requests.get(f"{self.SHOPIFY_API_URL}/products/{product_id}/images.json", headers=self.SHOPIFY_HEADERS)
        if response.status_code != 200:
            logger.error(f"Error fetching product images from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Fetched {len(response.json())} images from Shopify's product ID {product_id}")
        return response.json()

    def upload_image_to_shopify(self, image, product_id, variant_ids):
        data = {
            "image": {
                **image
            }
        }
        if variant_ids:
            data["image"]["variant_ids"] = variant_ids

        response = requests.post(f"{self.SHOPIFY_API_URL}/products/{product_id}/images.json", headers=self.SHOPIFY_HEADERS, json=data)
        return {
            "status": response.status_code,
            "response": response.json(),
            "image_alt": image.get("alt")
        }

    def get_product_variants(self, product_id: int, params: dict = {}):
        response = requests.get(f"{self.SHOPIFY_API_URL}/products/{product_id}/variants.json", params=params, headers=self.SHOPIFY_HEADERS)
        if response.status_code != 200:
            logger.error(f"Error fetching product variants from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Fetched {len(response.json())} variants from Shopify's product ID {product_id}")
        return response.json()

    def get_smart_collections(self, params: dict = {}):
        response = requests.get(f"{self.SHOPIFY_API_URL}/smart_collections.json", params=params, headers=self.SHOPIFY_HEADERS)
        if response.status_code != 200:
            logger.error(f"Error fetching smart collections from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Fetched {len(response.json())} smart collections from Shopify")
        return response.json()

    def create_smart_collection(self, data: dict):
        response = requests.post(f"{self.SHOPIFY_API_URL}/smart_collections.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 201:
            logger.error(f"Error creating smart collection in Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Created smart collection {data['smart_collection']['title']} in Shopify")
        return response.json()
