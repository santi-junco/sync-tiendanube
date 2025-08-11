import os
import time
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
        response = requests.post(f"{self.SHOPIFY_API_URL}/inventory_levels/set.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 200:
            logger.error(f"Error setting default inventory level in Shopify: {response.status_code} - {response.text} - {response.content}")
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
        else:
            collection_id = response.json()["smart_collection"]["id"]

            update_data = {
                "smart_collection": {
                    "id": collection_id,
                    "sort_order": "created-desc"
                }
            }

            requests.put(
                f"{self.SHOPIFY_API_URL}/smart_collections/{collection_id}.json",
                headers=self.SHOPIFY_HEADERS,
                json=update_data
            )
            logger.info(f"Created smart collection {data['smart_collection']['title']} in Shopify")
        return response.json()

    def get_products_by_vendor(self, vendor: str, params: dict = None):
        if params is None:
            params = {
                "vendor": vendor,
                "limit": 250
            }

        all_products = []
        next_page_info = None
        seen_page_info = set()

        while True:
            request_params = params.copy()
            if next_page_info:
                request_params = {"limit": 250, "page_info": next_page_info}

            response = requests.get(
                f"{self.SHOPIFY_API_URL}/products.json",
                params=request_params,
                headers=self.SHOPIFY_HEADERS
            )

            if response.status_code != 200:
                logger.error(
                    f"Error fetching products by vendor from Shopify: "
                    f"{response.status_code} - {response.text}"
                )
                break

            products = response.json().get("products", [])
            all_products.extend(products)
            logger.info(f"Fetched {len(products)} products (total so far: {len(all_products)})")

            # Analizar el header Link y buscar solo el rel="next"
            link_header = response.headers.get("Link", "")
            next_page_info = None

            if 'rel="next"' in link_header:
                import re
                matches = re.findall(r'<([^>]+)>; rel="next"', link_header)
                if matches:
                    next_url = matches[0]
                    match = re.search(r'page_info=([^&>]+)', next_url)
                    if match:
                        candidate_page_info = match.group(1)
                        if candidate_page_info not in seen_page_info:
                            seen_page_info.add(candidate_page_info)
                            next_page_info = candidate_page_info

            # Si no hay más páginas, salir
            if not next_page_info:
                break

        logger.info(f"Total products fetched for vendor {vendor}: {len(all_products)}")
        return all_products

    def delete_product(self, product_id: int, data: dict = None):
        """Esta funcion "elimina" un producto de shopify, pero en realidad lo desactiva
        y lo pone en modo "borrador"

        Args:
            product_id (int): ID del producto a eliminar

        Returns:
            dict: Respuesta de la API de Shopify
        """
        if data is None:
            data = {
                "product": {
                    "status": "draft"
                }
            }
        response = requests.put(f"{self.SHOPIFY_API_URL}/products/{product_id}.json", headers=self.SHOPIFY_HEADERS, json=data)
        if response.status_code != 200:
            logger.error(f"Error deleting product from Shopify: {response.status_code} - {response.text}")
            return {}
        logger.info(f"Deleted product {product_id} from Shopify")
        return response.json()

    def fetch_shopify_variants_by_handle(self, handle):
        params = {
            "fields": "variants",
            "handle": handle
        }
        time.sleep(0.3)
        response = self.get_products(params)
        variants = []
        for product in response.get("products", []):
            variants.extend(product.get("variants", []))
        return variants

    def process_variant_stock_update(self, tienda_config, tn_variant, sh_variant):
        expected_sku = str(tn_variant["id"])
        stock = tn_variant["stock"] if tn_variant["stock"] is not None else 999

        if sh_variant["sku"] == expected_sku and sh_variant["inventory_quantity"] != stock:
            logger.info(f"Updating stock for variant {sh_variant['id']}")
            data = {
                "location_id": tienda_config['deposit'],
                "inventory_item_id": sh_variant['inventory_item_id'],
                "available": stock
            }
            time.sleep(0.3)
            self.set_inventory_level(data)
