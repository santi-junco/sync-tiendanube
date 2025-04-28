import os
import json
import logging
import requests
import pytz

from dateutil import parser
from dotenv import load_dotenv
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta


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
    




@app.post("/sync-shopify")
def sync_shopify():
    try:
        logger.info("Request received at sync-shopify endpoint")
        
        # Calcular tiempo hace 15 minutos con formato ISO 8601
        hora_actual = datetime.now(pytz.UTC)
        quince_minutos = hora_actual - timedelta(minutes=15)
        updated_at_min = quince_minutos.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        productos_actualizados = []
        
        # Iterar sobre todas las tiendas configuradas
        for vendor, store_info in TIENDANUBE_STORES.items():
            logger.info(f"Processing store for vendor: {vendor}")
            
            params_products = {
                'per_page': 200,
                'updated_at_min': updated_at_min,
            }
            response_products = requests.get(
                f"{store_info['url']}/products",
                headers=store_info['headers'],
                params=params_products
            )
            if response_products.status_code == 200:
                products = response_products.json()
                logger.info(f"Found {len(products)} updated products for vendor {vendor}")
                for product in products:
                    product_id = product.get('id')
                    if not product_id:
                        continue

                    # Obtener solo las variantes modificadas de este producto
                    params_variants = {
                        'updated_at_min': updated_at_min,
                        'per_page': 200,
                        'fields': 'id,sku,stock,updated_at'
                    }
                    response_variants = requests.get(
                        f"{store_info['url']}/products/{product_id}/variants",
                        headers=store_info['headers'],
                        params=params_variants
                    )
                    if response_variants.status_code == 200:
                        variants = response_variants.json()
                        logger.info(f"Found {len(variants)} updated variants for product {product_id} (vendor {vendor})")
                        for variant in variants:
                            # Solo variantes con SKU válido
                            if not variant.get('sku'):
                                continue
                            try:
                                search_response = requests.get(
                                    f"{SHOPIFY_API_URL}/variants.json",
                                    headers={"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN},
                                    params={'sku': variant['sku']}
                                )
                                if search_response.status_code == 200:
                                    shopify_variants = search_response.json()['variants']
                                    if shopify_variants:
                                        shopify_variant = shopify_variants[0]
                                        update_response = requests.put(
                                            f"{SHOPIFY_API_URL}/variants/{shopify_variant['id']}.json",
                                            headers={"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN},
                                            json={
                                                'variant': {
                                                    'id': shopify_variant['id'],
                                                    'inventory_quantity': variant['stock']
                                                }
                                            }
                                        )
                                        if update_response.status_code == 200:
                                            logger.info(f"Updated variant stock: {variant['sku']} - New stock: {variant['stock']}")
                                            productos_actualizados.append({
                                                "vendor": vendor,
                                                "sku": variant['sku'],
                                                "new_stock": variant['stock']
                                            })
                                        else:
                                            logger.error(f"Error updating variant stock: {update_response.status_code} - {update_response.text}")
                                    else:
                                        logger.warning(f"Variant not found in Shopify: {variant['sku']}")
                                else:
                                    logger.error(f"Error searching Shopify variant: {search_response.status_code} - {search_response.text}")
                            except Exception as e:
                                logger.error(f"Error processing variant {variant.get('sku', 'N/A')}: {str(e)}")
                                continue
                    else:
                        logger.error(f"Error fetching variants from Tiendanube: {response_variants.status_code} - {response_variants.text}")
            else:
                logger.error(f"Error fetching products from Tiendanube: {response_products.status_code} - {response_products.text}")
        
        logger.info(f"Synchronization completed. Updated {len(productos_actualizados)} products")
        return {
            "message": "Sincronización exitosa",
            "productos_actualizados": productos_actualizados
        }
        
    except Exception as e:
        logger.exception("Error occurred during synchronization with Shopify")
        return {"error": "An error occurred during synchronization with Shopify"}
