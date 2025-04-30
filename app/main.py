import os
import json
import logging
import requests

from dotenv import load_dotenv
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


# Cargar variables de entorno desde el archivo .env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path, encoding="utf-8")

tiendas_raw = os.getenv("TIENDAS")
TIENDANUBE_STORES = json.loads(tiendas_raw)

# Configuración de Shopify
SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION")
SHOPIFY_API_URL = f"{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}"

# Crear directorio si no existe
os.makedirs("logs", exist_ok=True)

# Función para limpiar logs viejos
def eliminar_logs_viejos(directorio="logs", dias=5):
    hoy = datetime.now()
    for filename in os.listdir(directorio):
        if filename.endswith(".log"):
            try:
                fecha_str = filename.replace(".log", "")
                fecha_archivo = datetime.strptime(fecha_str, "%Y%m%d")
                if hoy - fecha_archivo > timedelta(days=dias):
                    os.remove(os.path.join(directorio, filename))
            except ValueError:
                # Ignorar archivos que no cumplan con el formato
                pass

# Ejecutar limpieza
eliminar_logs_viejos()

# Crear archivo de log para hoy
fecha_log = datetime.now().strftime("%Y%m%d")
log_filename = f"logs/{fecha_log}.log"

# Logger
logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(log_filename)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False

app = FastAPI()

# Scheduler
scheduler = BackgroundScheduler()

@app.get("/")
def root():
    try:
        logger.info("Request received at root endpoint")
        return {
            "api_name": "Sincronizacion de Tiendanube",
            "version": "1.0.0",
            "description": "API para recibir webhooks de Shopify y sincronizar con Tiendanube",
            "status": "online",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.exception("Error occurred at root endpoint")
        return {"error": "An error occurred"}


@app.post("/sync-tiendanube")
def sync(body: dict):
    try:
        logger.info("Request received at sync-tiendanube endpoint")
        # Obtengo los productos del pedido
        list_products = body.get("line_items", [])
        if not list_products:
            logger.warning("No products found in the order")
            return {"error": "No products found in the order"}

        pedidos = []

        # Recorro los productos del pedido
        logger.info(f"Number of products in the order: {len(list_products)}")
        for product in list_products:

            logger.info(f"Processing order: {product.get('id')}")
            pedido = {
                "vendor": product.get("vendor"),
                "quantity": product.get("quantity")
            }

            if not pedido['vendor']:
                logger.warning("Vendor not found in the product")
                # return {"error": "Vendor not found in the product"}

            logger.info(f"Obtaining product {product['product_id']} from Shopify")
            response = requests.get(
                url=f"{SHOPIFY_API_URL}/products/{product['product_id']}.json",
                headers={
                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                },
                params={
                    "fields": "handle",
                }
            )
            if response.status_code == 200:
                response_data = response.json()
                pedido['product_id'] = response_data['product']['handle']
            else:
                logger.error(f"Error fetching product data: {response.status_code} - {response.text}")
                return {"error": "Error fetching product data"}

            logger.info(f"Obtaining variant {product['variant_id']} from Shopify")
            response = requests.get(
                url=f"{SHOPIFY_API_URL}/variants/{product['variant_id']}.json",
                headers={
                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                },
                params={
                    "fields": "sku"
                }
            )

            if response.status_code == 200:
                response_data = response.json()
                pedido['variant_id'] = response_data['variant']['sku']
            else:
                logger.error(f"Error fetching variant data: {response.status_code} - {response.text}")
                return {"error": "Error fetching variant data"}

            pedidos.append(pedido)
            logger.info(f"Product {pedido['product_id']} with variant {pedido['variant_id']} and quantity {pedido['quantity']} added to the list")

        logger.info(f"Number of products to update: {len(pedidos)}")
        for pedido in pedidos:
            logger.info(f"Updating stock for product {pedido['product_id']} with variant {pedido['variant_id']} and quantity {pedido['quantity']} from vendor {pedido['vendor']}")
            url = f"{TIENDANUBE_STORES[str(pedido['vendor'])]['url']}/products/{pedido['product_id']}/variants/stock"
            headers = TIENDANUBE_STORES[str(pedido['vendor'])]['headers']
            data = {
                "action" : "variation",
                "value" : pedido['quantity'] * -1,
                "id" : pedido['variant_id']
            }

            logger.info(f"Sending request to {url} with data {data}")
            response = requests.post(url, headers=headers, json=data)
            if response.status_code != 200:
                logger.error(f"Error updating stock: {response.status_code} - {response.text}")
                return {"error": "Error updating stock"}
            else:
                logger.info(f"Stock updated successfully for product {pedido['product_id']} with variant {pedido['variant_id']} and quantity {pedido['quantity']} from vendor {pedido['vendor']}")

        logger.info("Stock updated successfully for all products in the order")
        logger.info("Synchronization completed successfully")

        return {"message": "Sincronización exitosa"}
    except Exception as e:
        logger.exception("Error occurred during synchronization")
        return {"error": "An error occurred during synchronization"}


def sync_stock():
    logger.info("Synchronizing stock...")
    try:
        # Obtengo los productos de Tiendanube
        updated_at_min = (datetime.now() - timedelta(minutes=15)).isoformat()
        for tienda in TIENDANUBE_STORES:
            logger.info(f"Fetching products from Tiendanube ID {tienda}")
            url = f"{TIENDANUBE_STORES[tienda]['url']}/products"
            headers = TIENDANUBE_STORES[tienda]['headers']
            params = {
                "per_page": 200,
                "published": "true",
                "min_stock": 1,
                "fields": "variants",
                "updated_at_min": updated_at_min
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                products_variants = response.json()
                logger.info(f"Fetched {len(products_variants)} variants from Tiendanube")
            else:
                logger.error(f"Error fetching variants from Tiendanube: {response.status_code} - {response.text}")

            tiendanube_variantes = []
            for product_variant in products_variants:
                for variant in product_variant.get("variants", []):
                    fecha_variante = datetime.strptime(variant["updated_at"], "%Y-%m-%dT%H:%M:%S%z").isoformat()
                    if fecha_variante >= updated_at_min:
                        tiendanube_variantes.append(variant)

            logger.info(f"Fetched {len(tiendanube_variantes)} filtered variants from Tiendanube")

            for tiendanube_variant in tiendanube_variantes:
                logger.info(f"Processing variant {tiendanube_variant['id']} from Tiendanube")
                url = f"{SHOPIFY_API_URL}/products.json"
                headers = {
                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                }
                params = {
                    "fields": "variants",
                    "handle": tiendanube_variant["product_id"]
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(f"Fetched {len(response_data)} products from Shopify")
                else:
                    logger.error(f"Error fetching products from Shopify: {response.status_code} - {response.text}")
                    continue

                shopify_variantes = []
                for product in response_data.get("products", []):
                    shopify_variantes.extend(product.get("variants", []))

                for shopify_variant in shopify_variantes:
                    logger.info(f"Processing variant {shopify_variant['id']} from Shopify")
                    if shopify_variant["sku"] == str(tiendanube_variant["id"]) and shopify_variant["inventory_quantity"] != tiendanube_variant["stock"]:
                        logger.info(f"Updating stock for variant {shopify_variant['id']} from Shopify")
                        url = f"{SHOPIFY_API_URL}/inventory_levels/set.json"
                        headers = {
                            "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                        }
                        data = {
                            "location_id": 104501772590,  # TODO por ahora esta herdcodeado pero hay que hacer la peticion para obtenerlo
                            "inventory_item_id": shopify_variant['inventory_item_id'],
                            "available": tiendanube_variant["stock"]
                        }

                        response = requests.post(url, headers=headers, json=data)
                        if response.status_code == 200:
                            logger.info(f"Stock updated successfully for variant {shopify_variant['id']} from Shopify")
                        else:
                            logger.error(f"Error updating stock: {response.status_code} - {response.text}")

    except Exception as e:
        logger.exception("Error occurred during stock synchronization")
        return {"error": "An error occurred during stock synchronization"}


@app.on_event("startup")
def start_scheduler():
    logger.info("Starting scheduler")
    scheduler.add_job(sync_stock, 'interval', minutes=10)
    scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    logger.info("Shutting down scheduler")
    scheduler.shutdown()