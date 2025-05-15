from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import os
import json
import logging
import requests
import certifi

from dotenv import load_dotenv
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

from PIL import Image
from rembg import remove


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
    logger.info("==========> Synchronizing stock... <==========")
    try:
        # Obtengo los productos de Tiendanube
        updated_at_min = (datetime.now() - timedelta(minutes=15)).isoformat()
        for tienda in TIENDANUBE_STORES:
            logger.info("#" * 50)
            logger.info(f"Fetching products from Tiendanube ID {tienda}")

            # Obtengo todas las variatnes de los productos de Tiendanube que fueron actualizadas en los ultimos 15 minutos
            products_variants = []
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
                logger.info(f"Fetched {len(products_variants)} products from Tiendanube")
            else:
                logger.error(f"Error fetching variants from Tiendanube: {response.status_code} - {response.text}")

            # Obtengo solamente las variantes que fueron actualizadas en los ultimos 15 minutos
            tiendanube_variantes = []
            for product_variant in products_variants:
                for variant in product_variant.get("variants", []):
                    fecha_variante = datetime.strptime(variant["updated_at"], "%Y-%m-%dT%H:%M:%S%z").isoformat()
                    if fecha_variante >= updated_at_min:
                        tiendanube_variantes.append(variant)
            logger.info(f"Fetched {len(tiendanube_variantes)} filtered variants from Tiendanube")

            # Obtengo las variantes del producto de Shopify por el handle que es el id del producto en Tiendanube
            for tiendanube_variant in tiendanube_variantes:
                logger.info(f"Getting product with handle {tiendanube_variant['product_id']} from Shopify")
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

                # Formateo la respuesta para obtener solamente una lista de variantes
                shopify_variantes = []
                for product in response_data.get("products", []):
                    shopify_variantes.extend(product.get("variants", []))

                # Recorro las variantes de Shopify, comparando el sku de la variante de Shopify con el id de la variante de Tiendanube
                # y si los stocks son diferentes, actualizo el stock de la variante de Shopify
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
        logger.exception("Error occurred during stock synchronization, Error: %s", str(e))
        return {"error": "An error occurred during stock synchronization"}


def calculate_price(price, promotional_price):
    precio = promotional_price if promotional_price else price
    # TODO va a ser mejor recibir este dict por params, pero cuando tenga definido para cada tienda
    RANGOS_PRECIO = [
        (0.00,     9000.00,   1.35),
        (9000.00,  10000.00,  1.28),
        (20000.00, 30000.00,  1.23),
        (30000.00, 40000.00,  1.19),
        (40000.00, 50000.00,  1.16),
        (50000.00, 60000.00,  1.14),
        (60000.00, 90000.00,  1.12),
        (100000.00, 199000.01, 1.1),
    ]

    try:
        precio = float(precio)
    except ValueError:
        logger.error(f"Invalid price format: {precio}")

    for inicio, fin, procentaje in RANGOS_PRECIO:
        if inicio <= precio < fin:
            return precio * procentaje

    return precio


def sync_products():
    logger.info("==========> Synchronizing products... <==========")
    try:
        for tienda in TIENDANUBE_STORES:
            logger.info("#" * 50)
            logger.info(f"Fetching products from Tiendanube ID {tienda}")

            updated_at_min = (datetime.now() - timedelta(days=1)).isoformat()

            # Obtengo los productos de Tiendanube
            products = []
            url = f"{TIENDANUBE_STORES[tienda]['url']}/products"
            headers = TIENDANUBE_STORES[tienda]['headers']

            # TODO ver como hacer cuando quieren traer cierta cantidad de productos
            # posible solucion: definir los params en el .env
            params = {
                "per_page": 200,
                "published": "true",
                "min_stock": 1,
                # "updated_at_min": updated_at_min  # TODO por el momento no lo uso pero despues va a ser cada 24 hrs
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                products = response.json()
                logger.info(f"Fetched {len(products)} products from Tiendanube")
            else:
                logger.error(f"Error fetching products from Tiendanube: {response.status_code} - {response.text}")

            for product in products:
                tiendanube_attributes = {}
                logger.info(f"Processing product {product['id']} from Tiendanube")
                # Creo un array con el valor de "attributes" de cada producto
                for index, attribute in enumerate(product.get("attributes", []), 1):
                    tiendanube_attributes[f"option{index}"] = attribute["es"]  # Cambié el nombre de la clave a "option{index}"

                logger.info(f"Attributes for product {product['id']}: {tiendanube_attributes}")

                # Creo un array de las variantes de cada producto
                tiendanube_variants = []
                relacion_variante_imagen = []
                for variant in product.get("variants", []):
                    stock = 0
                    # si el stock viene null es porque es infinito
                    if variant["stock"] == None:
                        stock = 999
                    if variant["stock"] > 0 :
                        stock = variant["stock"]

                    tiendanube_variants.append({
                        "sku": variant["id"],
                        "grams": None,
                        "price": calculate_price(variant["price"], variant["promotional_price"]),
                        "weight": variant["weight"],
                        "barcode": variant["barcode"],
                        "option1": tiendanube_attributes.get("option1", None),
                        "option2": tiendanube_attributes.get("option2", ""),
                        "option3": tiendanube_attributes.get("option3", ""),
                        "taxcode": None,
                        "position": variant["position"],
                        "weight_unit": None,
                        "compare_at_price": variant["compare_at_price"],
                        "inventory_policy": "deny",
                        "inventory_quantity": stock,
                        "presentment_prices": [],
                        "fulfillment_service": "manual",
                        "inventory_management": "shopify"
                    })

                    # Relaciono la variante con la imagen
                    # TODO aca se puede agregar el stock para actualizarlo si hace falta
                    relacion_variante_imagen.append({
                        "variant_id": variant["id"],  # es el sku de la variante en shopify
                        "image_id": variant["image_id"]  # es el alt de la imagen en shopify
                    })

                logger.info(f"Fetched {len(tiendanube_variants)} variants for product {product['id']}")

                # Creo un array con las imagenes de cada producto
                tiendanube_images = []
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futuros = {executor.submit(descargar_y_convertir, img): img for img in product.get("images", [])}
                    for futuro in as_completed(futuros):
                        data = futuro.result()
                        if data:
                            tiendanube_images.append(data)

                logger.info(f"Fetched {len(tiendanube_images)} images for product {product['id']}")

                # Obtengo los tags de cada producto
                tiendanube_tags = product.get("tags", '')
                tiendanube_tags += f", {tienda}, {TIENDANUBE_STORES[tienda]['category']}"
                for category in product.get("categories", []):
                    if category["name"]["es"] not in tiendanube_tags:
                        tiendanube_tags += f", {category['name']['es']}"
                tiendanube_tags = tiendanube_tags.split(",")

                logger.info(f"Tags for product {product['id']}: {tiendanube_tags}")

                # busco el producto en shopify por su handle, que es el id del producto en Tiendanube
                logger.info(f"Searching for product {product['id']} in Shopify")
                shopify_product = None
                url = f"{SHOPIFY_API_URL}/products.json"
                headers = {
                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                }
                params = {
                    "handle": product["id"]
                }

                response = requests.get(url, headers=headers, params=params, verify=certifi.where())
                if response.status_code == 200:
                    shopify_product = response.json().get("products", [])[0] if response.json().get("products", []) else []
                    logger.info(f"Fetched {len(shopify_product)} products from Shopify")
                else:
                    logger.error(f"Error fetching products from Shopify: {response.status_code} - {response.text}")
                    continue

                if shopify_product:
                    # Si el producto existe, lo actualizo
                    logger.info(f"Updating product {product['id']} in Shopify")
                    url = f"{SHOPIFY_API_URL}/products/{shopify_product['id']}.json"
                    headers = {
                        "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                    }
                    data = {
                        "product": {
                            "id": shopify_product['id'],
                            "handle": product["id"],
                            "title": product["name"]["es"],
                            "body_html": product["description"]["es"],
                            "vendor": tienda,
                            "product_type": TIENDANUBE_STORES[tienda]['category'],
                            "tags": tiendanube_tags,
                            "variants": tiendanube_variants,
                            "published": product["published"],
                            "options": tiendanube_attributes,
                            "images": tiendanube_images,
                        }
                    }

                    response = requests.put(url, headers=headers, json=data)
                    if response.status_code == 200:
                        logger.info(f"Product {product['id']} updated successfully in Shopify")
                    else:
                        logger.error(f"Error updating product: {response.status_code} - {response.text}")
                    logger.info(f"Product {product['id']} processed successfully")
                else:
                    # Si el producto no existe, lo creo
                    logger.info(f"Creating product {product['id']} in Shopify")
                    url = f"{SHOPIFY_API_URL}/products.json"
                    headers = {
                        "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                    }
                    data = {
                        "product": {
                            "title": product["name"]["es"],
                            "body_html": product["description"]["es"],
                            "vendor": tienda,
                            "product_type": TIENDANUBE_STORES[tienda]['category'],
                            "tags": tiendanube_tags,
                            "variants": tiendanube_variants,
                            "images": tiendanube_images,
                            "options": tiendanube_attributes,
                            "handle": product["id"],
                            "published": product["published"],
                            "status": "active"
                        }
                    }

                    response = requests.post(url, headers=headers, json=data)
                    if response.status_code == 201:
                        logger.info(f"Product {product['id']} created successfully in Shopify")
                    else:
                        logger.error(f"Error creating product: {response.status_code} - {response.text}")
                    # Si el producto se crea correctamente, actualizo las imagenes
                    if response.status_code == 201:
                        shopify_product = response.json().get("product", [])
                        logger.info(f"Updating images for product {product['id']} in Shopify")
                        for image in tiendanube_images:
                            # Busco la variante en shopify por el id de la variante en Tiendanube
                            variant_id = next((item for item in relacion_variante_imagen if item["image_id"] == image["alt"]), None)
                            if variant_id:
                                url = f"{SHOPIFY_API_URL}/products/{shopify_product['id']}/images.json"
                                headers = {
                                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                                }
                                data = {
                                    "image": {
                                        "variant_ids": [variant_id["variant_id"]],
                                        "position": image["position"],
                                        "attachment": image["data"].getvalue()
                                    }
                                }

                                response = requests.post(url, headers=headers, json=data)
                                if response.status_code == 201:
                                    logger.info(f"Image {image['alt']} updated successfully for product {product['id']} in Shopify")
                                else:
                                    logger.error(f"Error updating image: {response.status_code} - {response.text}")
                logger.info(f"Product {product['id']} processed successfully")

    except Exception as e:
        logger.exception("Error occurred during product synchronization, Error: %s", str(e))

# def descargar_y_convertir(img):
#     try:
#         response = requests.get(img['src'], stream=True)
#         if response.status_code == 200:
#             image_data = BytesIO(response.content)
#             image_data.seek(0)
#             return {
#                 "alt": img["id"],
#                 "data": str(image_data.getvalue()),
#                 "position": img["position"],
#             }
#     except Exception as e:
#         logger.error(f"Error downloading image {img['src']}: {e}")
#         return None


def descargar_y_convertir(img, gray_color=(120, 120, 120)):
    try:
        response = requests.get(img['src'], stream=True)
        response.raise_for_status()

        # Abrir la imagen original
        img_original = Image.open(BytesIO(response.content))

        # Eliminar el fondo
        img_no_bg = remove(img_original)

        # Crear fondo gris
        img_with_gray_bg = Image.new('RGB', img_no_bg.size, gray_color)

        # Pegar imagen sin fondo encima del gris
        if img_no_bg.mode == 'RGBA':
            img_with_gray_bg.paste(img_no_bg, (0, 0), img_no_bg)
        else:
            img_with_gray_bg.paste(img_no_bg, (0, 0))

        # Guardar en memoria
        output = BytesIO()
        img_with_gray_bg.save(output, format='PNG')
        output.seek(0)

        return {
            "alt": img["id"],
            "data": str(output.getvalue()),  # o usar base64 si prefieres
            "position": img["position"],
        }

    except Exception as e:
        logger.error(f"Error procesando imagen {img['src']}: {e}")
        return None


def build_full_handle(category, category_by_id):
    handles = []
    current = category
    while current:
        handles.insert(0, current["handle"]["es"])  # prepend
        parent_id = current["parent"]
        current = category_by_id.get(parent_id) if parent_id else None
    return "-".join(handles)


def create_smart_collections():
    logger.info("==========> Creating smart collections... <==========")
    logger.info("Fetching categories from Tiendanube")

    # Obtengo las colecciones de Shopify
    url = f"{SHOPIFY_API_URL}/smart_collections.json"
    headers = {
        "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
    }
    params = {
        "fields": "handle",
        "limit": 250
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        categories = response.json().get("smart_collections", [])
        logger.info(f"Fetched {len(categories)} categories from Shopify")
    else:
        logger.error(f"Error fetching categories from Shopify: {response.status_code} - {response.text}")

    shopify_collections = [category["handle"] for category in categories]

    for tienda in TIENDANUBE_STORES:
        logger.info("#" * 50)
        logger.info(f"Fetching categories from Tiendanube ID {tienda}")

        # Obtengo las categorias de Tiendanube
        url = f"{TIENDANUBE_STORES[tienda]['url']}/categories"
        headers = TIENDANUBE_STORES[tienda]['headers']
        params = {
            "per_page": 200,
        }
        response = requests.get(url, headers=headers, params=params)        
        if response.status_code == 200:
            categories = response.json()
            logger.info(f"Fetched {len(categories)} categories from Tiendanube")
        else:
            logger.error(f"Error fetching categories from Tiendanube: {response.status_code} - {response.text}")

        category_by_id = {cat["id"]: cat for cat in categories}

        # Construir las colecciones con handle completo desde raíz
        smart_collections_full_hierarchy = []

        for category in categories:
            name = category["name"]["es"]
            full_handle = build_full_handle(category, category_by_id)

            # Verificar si la colección ya existe en Shopify
            if full_handle in shopify_collections:
                logger.info(f"Collection {full_handle} already exists in Shopify")
                continue

            smart_collections_full_hierarchy.append({
                "smart_collection": {
                    "title": name,
                    "handle": full_handle,
                    "rules": [
                        {
                            "column": "tag",
                            "relation": "equals",
                            "condition": handle
                        } for handle in full_handle.split("-")
                    ],
                    "published": True
                }
            })

        # TODO momento de pegarle a shopify para crear las colecciones 


@app.on_event("startup")
def start_scheduler():
    logger.info("Starting scheduler")
    # scheduler.add_job(sync_stock, 'interval', minutes=15)
    # scheduler.add_job(sync_products, 'interval', seconds=15)
    scheduler.add_job(create_smart_collections, 'interval', seconds=10)
    scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    logger.info("Shutting down scheduler")
    scheduler.shutdown()