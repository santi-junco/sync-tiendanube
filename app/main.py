import time
import os
import json
import logging
import requests
import certifi
import html

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
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
STOP_WORDS = {"y", "de", "la", "el", "los", "las", "para", "a", "en"}

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
    start_time = time.time()
    logger.info("==========> Synchronizing products... <==========")
    try:
        for tienda in TIENDANUBE_STORES:
            logger.info("#" * 50)
            logger.info(f"Fetching products from {TIENDANUBE_STORES[tienda]['name']}")

            updated_at_min = (datetime.now() - timedelta(days=1)).isoformat()

            # Obtengo los productos de Tiendanube
            products = []
            headers = TIENDANUBE_STORES[tienda]['headers']
            products_quantity = TIENDANUBE_STORES[tienda].get('products_quantity')

            fetched = 0
            page = 1
            products = []

            while True:
                remaining = products_quantity - fetched if products_quantity else 200
                per_page = min(200, remaining)
                params = {
                    "per_page": per_page,
                    "page": page,
                    "published": "true",
                    "min_stock": 1,
                    "sort_by": "created-at-descending",
                    # "updated_at_min": updated_at_min  # Se puede agregar luego si es necesario
                }

                url = f"{TIENDANUBE_STORES[tienda]['url']}/products"
                response = requests.get(url, headers=headers, params=params)

                if response.status_code != 200:
                    logger.error(f"Error fetching products (page {page}) from Tiendanube: {response.status_code} - {response.text}")
                    break


                current_products = response.json()
                if not current_products:
                    break

                products.extend(current_products)
                fetched += len(current_products)

                logger.info(f"Fetched {len(current_products)} products on page {page} (Total: {fetched})")

                # Si se especificó un límite y lo alcanzamos, cortamos
                if products_quantity and fetched >= products_quantity:
                    products = products[:products_quantity]
                    break

                # Si trajo menos de per_page, ya no hay más páginas
                if len(current_products) < per_page:
                    break

                page += 1

            for product in products:

                logger.info(f"Processing product {product['id']} from Tiendanube")

                # Creo un array de las variantes de cada producto
                tiendanube_variants = []
                relacion_variante_imagen = []
                for variant in product.get("variants", []):
                    stock = variant["stock"] if variant["stock"] is not None else 999
                    values = [v.get("es") for v in variant.get("values", [])]
                    option1 = values[0] if len(values) > 0 else None
                    option2 = values[1] if len(values) > 1 else None
                    option3 = values[2] if len(values) > 2 else None

                    tiendanube_variants.append({
                        "sku": variant["id"],
                        "grams": None,
                        "price": calculate_price(variant["price"], variant["promotional_price"]),
                        "weight": variant["weight"],
                        "barcode": variant["barcode"],
                        "option1": option1,
                        "option2": option2,
                        "option3": option3,
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

                tiendanube_images = []
                for img in product.get("images", []):
                    imagen = preparar_imagen_por_src(img)
                    if imagen:
                        tiendanube_images.append(imagen)

                existing_tags = set(product.get("tags", "").split(","))

                # Limpiá espacios (por si vienen tags con espacio al principio o final)
                existing_tags = {tag.strip() for tag in existing_tags if tag.strip()}

                # Agregá las categorías si no están ya
                for handle_category in product.get("categories", []):
                    tag_name = handle_category["handle"]["es"].strip()
                    if tag_name not in existing_tags:
                        existing_tags.add(tag_name)

                existing_tags.add(tienda)
                existing_tags.add(TIENDANUBE_STORES[tienda]['category'])

                # Convertilo de nuevo a lista si necesitás
                tiendanube_tags = list(existing_tags)

                logger.info(f"Tags for product {product['id']}: {tiendanube_tags}")

                # busco el producto en shopify por su handle, que es el id del producto en Tiendanube
                logger.info(f"Searching for product {product['id']} in Shopify")
                time.sleep(1)
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

                # formateo los atributos = options
                tiendanube_attributes = [{"name": attr.get("es")} for attr in product.get("attributes", [])]
                if not tiendanube_attributes:
                    tiendanube_attributes.append({
                        "name": "Title"
                    })

                product_description = product["description"]["es"]
                soup = BeautifulSoup(product_description, "html.parser")
                text_only = soup.get_text(separator="\n")
                product_description = html.unescape(text_only)

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
                            "body_html": product_description,
                            "vendor": tienda,
                            "product_type": TIENDANUBE_STORES[tienda]['category'],
                            "tags": tiendanube_tags,
                            "variants": tiendanube_variants,
                            "published": product["published"],
                            "options": tiendanube_attributes or shopify_product['options']
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
                            "handle": product["id"],
                            "options": tiendanube_attributes,
                            "body_html": product_description,
                            "vendor": tienda,
                            "product_type": TIENDANUBE_STORES[tienda]['category'],
                            "tags": tiendanube_tags,
                            "published": product["published"],
                            "status": "active",
                            "variants": tiendanube_variants
                        }
                    }

                    response = requests.post(url, headers=headers, json=data)

                    if response.status_code == 201:
                        logger.info(f"Product {product['id']} created successfully in Shopify")
                    else:
                        logger.error(f"Error creating product: {response.status_code} - {response.text}")
                # Si el producto se crea correctamente, actualizo las imagenes
                if response.status_code in [200, 201]:

                    shopify_product = response.json().get("product", [])

                # Mapear variantes de Tiendanube (por SKU) a IDs de variantes en Shopify
                shopify_variant_map = {}
                for variant in shopify_product.get("variants", []):
                    shopify_variant_map[str(variant.get("sku"))] = variant.get("id")

                logger.info(f"Updating images for product {product['id']} in Shopify")

                prod_img_shopify = set()
                url = f"{SHOPIFY_API_URL}/products/{shopify_product['id']}/images.json"
                headers = {
                    "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
                }

                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    prod_img_shopify = {str(img.get("alt")) for img in response.json().get("images", [])}

                logger.info(f"Fetched {len(prod_img_shopify)} images from Shopify's product ID {shopify_product['id']}")

                images_to_upload = [
                    img for img in tiendanube_images
                    if str(img.get("alt")) not in prod_img_shopify
                ]
                logger.info(f"{len(images_to_upload)} images to load to Shopify")

                def upload_image_to_shopify(image, variant_ids, shopify_url, headers):
                    data = {
                        "image": {
                            **image
                        }
                    }
                    if variant_ids:
                        data["image"]["variant_ids"] = variant_ids

                    response = requests.post(shopify_url, json=data, headers=headers)
                    return {
                        "status": response.status_code,
                        "response": response.json(),
                        "image_alt": image.get("alt")
                    }

                futures = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    for image in images_to_upload:
                        image_id = image.get("alt")
                        # ⚠️ Convertir los variant_ids de Tiendanube a los de Shopify (vía SKU)
                        variant_ids = [
                            shopify_variant_map.get(str(rel["variant_id"]))
                            for rel in relacion_variante_imagen
                            if rel["image_id"] == image_id and shopify_variant_map.get(str(rel["variant_id"])) is not None
                        ]

                        futures.append(
                            executor.submit(upload_image_to_shopify, image, variant_ids, url, headers)
                        )

                    for future in as_completed(futures):
                        result = future.result()
                        print(f"Image {result['image_alt']} -> Status: {result['status']}")
                        if result["status"] != 200:
                            print(f"❌ Error: {result['response']}")


                logger.info(f"Product {product['id']} processed successfully")

    except Exception as e:
        logger.exception("Error occurred during product synchronization, Error: %s", str(e))

    end_time = time.time()
    duracion = end_time - start_time

    horas = int(duracion // 3600)
    minutos = int((duracion % 3600) // 60)
    segundos = duracion % 60

    logger.info(f"Tiendanube ID {tienda} products were created/updated in {horas}h {minutos}m {segundos:.2f}s")


def preparar_imagen_por_src(img):
    try:
        return {
            "src": img['src'],
            "alt": img.get("id", ""),
            "position": img.get("position", 1)
        }
    except Exception as e:
        logger.error(f"Error procesando imagen {img.get('src', '')}: {e}")
        return None


def build_full_handle(category_gral, category, category_by_id):
    handles = []
    current = category
    while current:
        handles.insert(0, current["handle"]["es"])  # prepend
        parent_id = current["parent"]
        current = category_by_id.get(parent_id) if parent_id else None
    handles.insert(0, category_gral)
    return ",".join(handles)


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

        for category in categories:
            smart_collections_full_hierarchy = {}
            name = category["name"]["es"]
            full_handle = build_full_handle(TIENDANUBE_STORES[tienda]['category'], category, category_by_id)

            # Verificar si la colección ya existe en Shopify
            if full_handle in shopify_collections:
                logger.info(f"Collection {full_handle} already exists in Shopify")
                continue

            smart_collections_full_hierarchy = {
                "smart_collection": {
                    "title": name,
                    "handle": full_handle.replace(',', '-'),
                    "rules": [
                        {
                            "column": "tag",
                            "relation": "equals",
                            "condition": handle
                        } for handle in full_handle.split(",")
                    ],
                    "published": True
                }
            }

            url = f"{SHOPIFY_API_URL}/smart_collections.json"
            headers = {
                "X-Shopify-Access-Token": f"{SHOPIFY_ACCESS_TOKEN}",
            }

            response = requests.post(url=url, headers=headers, json=smart_collections_full_hierarchy)

            if response.status_code == 201:
                logger.info(f"Smart collection {name} created successfully")
            else:
                logger.error(f"Error creating smart collection {name}: {response.status_code} - {response.text} - {smart_collections_full_hierarchy}")


def collection_and_products():
    create_smart_collections()
    sync_products()


@app.on_event("startup")
def start_scheduler():
    logger.info("Starting scheduler")
    scheduler.add_job(sync_stock, 'interval', minutes=15)
    scheduler.add_job(collection_and_products, 'interval', hours=6, next_run_time=datetime.now() + timedelta(minutes=1))
    scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    logger.info("Shutting down scheduler")
    scheduler.shutdown()