import time
import os
import json
import html

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from apscheduler.schedulers.background import BackgroundScheduler

from app.logger import logger
from app.Shopify import Shopify
from app.Tiendanube import Tiendanube
from app.utils import calculate_execution_time, build_full_handle, preparar_imagen_por_src, calculate_price, create_tags

# Cargar variables de entorno desde el archivo .env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path, encoding="utf-8")

tiendas_raw = os.getenv("TIENDAS")
TIENDANUBE_STORES = json.loads(tiendas_raw)


app = FastAPI()

# Scheduler
scheduler = BackgroundScheduler()
tiendanube = Tiendanube()
shopify = Shopify()


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
        logger.exception(f"Error occurred at root endpoint: {e}")
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
            params = {
                "fields": "handle",
            }
            response = shopify.get_product(product['product_id'], params=params)
            if response:
                pedido['product_id'] = response['product']['handle']

            logger.info(f"Obtaining variant {product['variant_id']} from Shopify")
            params = {
                "fields": "sku"
            }
            response = shopify.get_product_variants(product['variant_id'], params=params)
            if response:
                pedido['variant_id'] = response['variants'][0]['sku']

            pedidos.append(pedido)
            logger.info(f"Product {pedido['product_id']} with variant {pedido['variant_id']} and quantity {pedido['quantity']} added to the list")

        logger.info(f"Number of products to update: {len(pedidos)}")
        for pedido in pedidos:
            logger.info(f"Updating stock for product {pedido['product_id']} with variant {pedido['variant_id']} and quantity {pedido['quantity']} from vendor {pedido['vendor']}")
            url = f"{TIENDANUBE_STORES[str(pedido['vendor'])]['url']}/products/{pedido['product_id']}/variants/stock"
            headers = TIENDANUBE_STORES[str(pedido['vendor'])]['headers']
            data = {
                "action": "variation",
                "value": pedido['quantity'] * -1,
                "id": pedido['variant_id']
            }

            logger.info(f"Sending request to {url} with data {data}")
            # TODO ver de mejorar el mensaje dentro de la funcion de tiendanube o despues de la funcion
            response = tiendanube.update_stock(url, headers, data)

        logger.info("Stock updated successfully for all products in the order")
        logger.info("Synchronization completed successfully")

        return {"message": "Sincronización exitosa"}
    except Exception as e:
        logger.exception(f"Error occurred during synchronization: {e}")
        return {"error": "An error occurred during synchronization"}


def sync_products():
    start_time = time.time()
    logger.info("==========> Synchronizing products... <==========")
    try:
        for tienda in TIENDANUBE_STORES:
            logger.info("#" * 50)
            logger.info(f"Fetching products from {TIENDANUBE_STORES[tienda]['name']}")

            updated_at_min = (datetime.now() - timedelta(hours=6)).isoformat()

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
                }

                if not products_quantity:
                    params["updated_at_min"] = updated_at_min

                url = f"{TIENDANUBE_STORES[tienda]['url']}/products"
                current_products = tiendanube.get_products(url, headers, params)
                if not current_products:
                    break

                products.extend(current_products)
                fetched += len(current_products)

                # Si se especificó un límite y lo alcanzamos, cortamos
                if products_quantity and fetched >= products_quantity:
                    products = products[:products_quantity]
                    break

                # Si trajo menos de per_page, ya no hay más páginas
                if len(current_products) < per_page:
                    break

                page += 1

            if products_quantity:
                filtered_data = []
                products_from_shopify = shopify.get_products_by_vendor(tienda).get("products", [])
                products_to_eliminate = []
                products_id_from_tiendanube = [str(p.get("id")) for p in products]
                for product in products_from_shopify:
                    if product.get("handle") not in products_id_from_tiendanube:
                        products_to_eliminate.append(product)
                logger.info(f"Products to eliminate: {len(products_to_eliminate)}")
                for product in products_to_eliminate:
                    shopify.delete_product(product.get("id"))

                for product in products:
                    if product.get('updated_at') and product['updated_at'] >= updated_at_min:
                        filtered_data.append(product)
                        continue

                    for variant in product.get('variants', []):
                        if variant.get('updated_at') and variant['updated_at'] >= updated_at_min:
                            filtered_data.append(product)
                            break

                products = filtered_data

            logger.info(f"Total products to update: {len(products)}")

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
                    tag_handle = handle_category["handle"]["es"].strip()
                    tag_name = handle_category["name"]["es"].strip()
                    if tag_handle not in existing_tags:
                        existing_tags.add(tag_handle)
                    if tag_name not in existing_tags:
                        existing_tags.add(tag_name)

                existing_tags.add(tienda)
                existing_tags.add(TIENDANUBE_STORES[tienda]['category'])
                existing_tags.add(TIENDANUBE_STORES[tienda].get('category_2', ''))

                # Convertilo de nuevo a lista si necesitás
                tiendanube_tags = []
                tiendanube_tags = create_tags(existing_tags)

                logger.info(f"Tags for product {product['id']}: {tiendanube_tags}")

                # busco el producto en shopify por su handle, que es el id del producto en Tiendanube
                logger.info(f"Searching for product {product['id']} in Shopify")
                time.sleep(1)
                shopify_product = None
                params = {
                    "handle": product["id"]
                }
                shopify_product = shopify.get_products(params)
                shopify_product = shopify_product.get("products", [])[0] if shopify_product.get("products", []) else []

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

                    response = shopify.update_product(shopify_product['id'], data)

                else:
                    # Si el producto no existe, lo creo
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

                    response = shopify.create_product(data)

                # Si el producto se crea correctamente, actualizo las imagenes
                if response:
                    shopify_product = response.get("product", [])
                    shopify_product_variants = shopify_product.get("variants", [])

                    # Mapear variantes de Tiendanube (por SKU) a IDs de variantes en Shopify
                    shopify_variant_map = {}
                    for variant in shopify_product_variants:
                        # shopify_variant_map[str(variant.get("sku"))] = variant.get("id")
                        sku = str(variant.get("sku"))
                        shopify_variant_map[sku] = variant.get("id")

                        inventory_item_id = variant.get("inventory_item_id")
                        if not inventory_item_id:
                            continue  # Evitar errores si no viene

                        # Buscar el stock correspondiente a este SKU
                        tiendanube_stock_variant = next(
                            (v for v in product.get("variants", []) if str(v["id"]) == sku),
                            None
                        )
                        if not tiendanube_stock_variant:
                            continue

                        stock = tiendanube_stock_variant.get("stock") or 999
                        if variant.get("inventory_quantity") == stock and TIENDANUBE_STORES[tienda]['deposit'] == shopify.DEFAULT_DEPOSIT:
                            logger.info(f"Stock for variant {variant['id']} is already up to date in Shopify")
                            continue

                        data = {
                            "location_id": TIENDANUBE_STORES[tienda]['deposit'],
                            "inventory_item_id": variant['inventory_item_id'],
                            "available": stock
                        }
                        time.sleep(1)  # Evitar rate limit de Shopify
                        response = shopify.set_inventory_level(data)
                        if response:
                            logger.info(f"Stock updated successfully for variant {variant['id']} from Shopify")

                        if TIENDANUBE_STORES[tienda]['deposit'] != shopify.DEFAULT_DEPOSIT:
                            response = shopify.set_default_inventory_level(variant['inventory_item_id'])
                            if response:
                                logger.info(f"Stock updated successfully for variant {variant['id']} from Shopify")

                logger.info(f"Updating images for product {product['id']} in Shopify")

                prod_img_shopify = set()
                response = shopify.get_product_images(shopify_product['id'])
                if response:
                    prod_img_shopify = {str(img.get("alt")) for img in response.get("images", [])}

                # logger.info(f"Fetched {len(prod_img_shopify)} images from Shopify's product ID {shopify_product['id']}")

                images_to_upload = [
                    img for img in tiendanube_images
                    if str(img.get("alt")) not in prod_img_shopify
                ]
                logger.info(f"{len(images_to_upload)} images to load to Shopify")

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
                            # executor.submit(upload_image_to_shopify, image, variant_ids, url, headers)
                            executor.submit(shopify.upload_image_to_shopify, image, shopify_product['id'], variant_ids)
                        )

                    for future in as_completed(futures):
                        result = future.result()
                        print(f"Image {result['image_alt']} -> Status: {result['status']}")
                        if result["status"] != 200:
                            print(f"Error: {result['response']}")

                logger.info(f"Product {product['id']} processed successfully")

    except Exception as e:
        logger.exception("Error occurred during product synchronization, Error: %s", str(e))

    end_time = time.time()

    logger.info(f"Products were created/updated in {calculate_execution_time(start_time, end_time)}")


def create_smart_collections():
    logger.info("==========> Creating smart collections... <==========")
    logger.info("Fetching categories from Tiendanube")

    params = {
        "fields": "handle",
        "limit": 250
    }
    response = shopify.get_smart_collections(params)
    if response:
        categories = response.get("smart_collections", [])

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
        categories = tiendanube.get_categories(url, headers, params)

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

            shopify.create_smart_collection(smart_collections_full_hierarchy)


categories_to_create = [
    ("indumentaria", "hombre", ["pantalon", "remera", "camisa", "abrigo", "otro"]),
    ("indumentaria", "mujer", ["pantalon", "remera", "camisa", "abrigo", "otro"]),
    ("indumentaria", "nino", ["pantalon", "remera", "camisa", "abrigo", "otro"]),

    ("bazar", "manteleria", ["mantel", "repasador", "servilleta", "otro"]),
    ("bazar", "cristaleria", ["otro"]),

    ("electronica", "celulares", ["accesorios", "otro"]),
    ("electronica", "computadora", ["accesorios", "otro"]),
    ("electronica", "reloj", ["accesorios", "otro"]),

    ("perfumeria", "hombre", ["perfume", "otro"]),
    ("perfumeria", "mujer", ["perfume", "otro"]),
]


def create_collections(categories_to_create):
    # Obtengo las collections que ya estan creadas
    params = {
        "fields": "handle",
        "limit": 250
    }
    response = shopify.get_smart_collections(params)
    if response:
        shopify_collections = response.get("smart_collections", [])
    shopify_collections = [collection["handle"] for collection in shopify_collections]

    for cat_general, second_level, specifics in categories_to_create:
        if cat_general in shopify_collections:
            logger.info(f"Collection {cat_general} already exists in Shopify")
        else:
            # Nivel 1: solo categoría general
            collections = {
                "smart_collection": {
                    "title": cat_general,
                    "handle": cat_general,
                    "rules": [{"column": "tag", "relation": "equals", "condition": cat_general}],
                    "published": True
                }
            }
            time.sleep(0.35)
            shopify.create_smart_collection(collections)

        if f"{cat_general}-{second_level}" in shopify_collections:
            logger.info(f"Collection {cat_general}-{second_level} already exists in Shopify")
        else:
            # Nivel 2: general + segundo nivel
            collections = {
                "smart_collection": {
                    "title": f"{second_level}",
                    "handle": f"{cat_general}-{second_level}",
                    "rules": [
                        {"column": "tag", "relation": "equals", "condition": cat_general},
                        {"column": "tag", "relation": "equals", "condition": second_level}
                    ],
                    "published": True
                }
            }
            time.sleep(0.35)
            shopify.create_smart_collection(collections)

        # Nivel 3: + específica
        for specific in specifics:
            if f"{cat_general}-{second_level}-{specific}" in shopify_collections:
                logger.info(f"Collection {cat_general}-{second_level}-{specific} already exists in Shopify")
            else:

                collections = {
                    "smart_collection": {
                        "title": f"{specific}",
                        "handle": f"{cat_general}-{second_level}-{specific}",
                        "rules": [
                            {"column": "tag", "relation": "equals", "condition": cat_general},
                            {"column": "tag", "relation": "equals", "condition": second_level},
                            {"column": "tag", "relation": "equals", "condition": specific}
                        ],
                        "published": True
                    }
                }
                time.sleep(0.35)
                shopify.create_smart_collection(collections)


def collection_and_products():
    create_collections(categories_to_create)
    sync_products()


def sync_stock():
    start_time = time.time()
    logger.info("==========> Synchronizing stock... <==========")

    updated_at_min = (datetime.now() - timedelta(minutes=15)).isoformat()

    for tienda_key, tienda_config in TIENDANUBE_STORES.items():
        logger.info(f"Fetching products from {tienda_config['name']}")

        tiendanube_variants = tiendanube.fetch_recent_variants(tienda_config, updated_at_min)
        logger.info(f"Fetched {len(tiendanube_variants)} filtered variants from Tiendanube")

        for tn_variant in tiendanube_variants:
            handle = tn_variant['product_id']
            logger.info(f"Getting product with handle {handle} from Shopify")

            shopify_variants = shopify.fetch_shopify_variants_by_handle(handle)

            for sh_variant in shopify_variants:
                shopify.process_variant_stock_update(tienda_config, tn_variant, sh_variant)

    end_time = time.time()
    logger.info(f"Stock sync completed in {calculate_execution_time(start_time, end_time)}")


@app.on_event("startup")
def start_scheduler():
    logger.info("Starting scheduler")
    scheduler.add_job(sync_stock, 'interval', minutes=15, id='sync_stock_job', max_instances=1, coalesce=True)
    scheduler.add_job(collection_and_products, 'interval', hours=6, next_run_time=datetime.now() + timedelta(minutes=1))
    scheduler.start()


@app.on_event("shutdown")
def shutdown_scheduler():
    logger.info("Shutting down scheduler")
    scheduler.shutdown()
