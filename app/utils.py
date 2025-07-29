import re
import unicodedata

from app.logger import logger

RANGOS_PRECIO = [
    (0.00, 8999.99, 1.35),
    (9000.00, 19999.99, 1.30),
    (20000.00, 29999.99, 1.25),
    (30000.00, 39999.99, 1.22),
    (40000.00, 49999.99, 1.19),
    (50000.00, 59999.99, 1.16),
    (60000.00, 99999.99, 1.14),
    (100000.00, 199999.99, 1.12),
]


TAGS_EQUIVALENCIA = {
    "indumentaria": {
        # pantalones
        "pantalon": "pantalon",
        "pantalones": "pantalon",
        "short": "pantalon",
        "shorts": "pantalon",
        "bermuda": "pantalon",
        "bermudas": "pantalon",
        "jogger": "pantalon",
        "joggers": "pantalon",
        "jogging": "pantalon",
        "joggings": "pantalon",

        # remeras
        "remera": "remera",
        "remeras": "remera",
        "chomba": "remera",
        "chombas": "remera",
        "camiseta": "remera",
        "camisetas": "remera",

        # camisas
        "camisa": "camisa",
        "camisas": "camisa",

        # abrigo
        "abrigos": "abrigo",
        "abrigo": "abrigo",
        "campera": "abrigo",
        "camperas": "abrigo",
        "camperita": "abrigo",
        "camperitas": "abrigo",
        "buzo": "abrigo",
        "buzos": "abrigo",
    },
    "bazar": {
        "repasador": "manteleria",
        "repasadores": "manteleria",

        "servilleta": "manteleria",
        "servilletas": "manteleria",

        "mantel": "manelteria",
        "manteles": "manelteria",
        "manteleria": "manelteria",
        "mantelerias": "manelteria",
        "individual": "manelteria",
        "individuales": "manelteria",
        "camino de mesa": "manelteria",
        "caminos de mesa": "manelteria",

    },
    "textil hogar": {
        "repasador": "manteleria",
        "repasadores": "manteleria",

        "servilleta": "manteleria",
        "servilletas": "manteleria",

        "mantel": "manelteria",
        "manteles": "manelteria",
        "manteleria": "manelteria",
        "mantelerias": "manelteria",
        "individual": "manelteria",
        "individuales": "manelteria",
        "camino de mesa": "manelteria",
        "caminos de mesa": "manelteria",
    },
    "electronica": {
        "celular": "celular",
        "celulares": "celular",
        "smartphone": "celular",
        "smartphones": "celular",

        "smartwatch": "reloj",
        "smartwatches": "reloj",
        "reloj": "reloj",
        "relojes": "reloj",

        "malla": "accesorios",
        "mallas": "accesorios",
        "accesorio": "accesorios",
        "accesorios": "accesorios",
        "cable": "accesorios",
        "cables": "accesorios",
        "cargador": "accesorios",
        "cargadores": "accesorios",
        "auricular": "accesorios",
        "auriculares": "accesorios",
    },
    "perfumeria": {
        "perfume": "perfume",
        "perfumes": "perfume",
        "perfum": "perfume",
        "perfums": "perfume",
    },
    "blanqueria": {
        "sabana": "ropa-cama",
        "sabanas": "ropa-cama",
        "acolchado": "ropa-cama",
        "acolchados": "ropa-cama",
        "frazada": "ropa-cama",
        "frazadas": "ropa-cama",
        "almohada": "ropa-cama",
        "almohadas": "ropa-cama",

        "alfombra": "alfombra",
        "alfombras": "alfombra",

        "cortina": "cortina",
        "cortinas": "cortina",

        "toalla": "toalla",
        "toallas": "toalla",
        "toallon": "toalla",
        "toallones": "toalla",
    },
    "valija bolso": {
        "accesorios": "accesorio",

        "mochilas": "mochila",
        "mochila": "mochila",

        "matera": "matera",
        "materas": "matera",

        "portafolio": "portafolio",
        "portafolios": "portafolio",

        "valija": "valija",
        "valijas": "valija",

        "bolso": "bolso",
        "bolsos": "bolso",
    }
}

CATEGORIES_TO_CREATE = [
    ("indumentaria", "hombre", ["pantalon", "remera", "camisa", "abrigo", "otro"]),
    ("indumentaria", "mujer", ["pantalon", "remera", "camisa", "abrigo", "otro"]),
    ("indumentaria", "nino", ["pantalon", "remera", "camisa", "abrigo", "otro"]),

    ("bazar", "manteleria", ["mantel", "repasador", "servilleta", "otro"]),
    ("bazar", "cristaleria", ["otro"]),
    ("bazar", "cocina", ["otro"]),
    ("bazar", "bano", ["otro"]),

    ("electronica", "celulares", ["accesorios", "otro"]),
    ("electronica", "computadora", ["accesorios", "otro"]),
    ("electronica", "reloj", ["accesorios", "otro"]),

    ("perfumeria", "hombre", ["perfume", "otro"]),
    ("perfumeria", "mujer", ["perfume", "otro"]),

    ("blanqueria", "ropa-cama", ["sabana", "frazada", "acolchado", "almohada", "otro"]),
    ("blanqueria", "alfombra", ["otro"]),
    ("blanqueria", "cortina", ["otro"]),
    ("blanqueria", "toalla", ["otro"]),

    ("valija-bolso", "accesorio", ["de-viaje", "beauty", "otro"]),
    ("valija-bolso", "mochila", ["portanotebook", "urbana", "materas", "otro"]),
    ("valija-bolso", "portafolio", ["chico", "mediano", "grande", "otro"]),
    ("valija-bolso", "valija", ["carry-on", "mediana", "grande", "set-valijas", "otro"]),
    ("valija-bolso", "bolso", ["otro"]),
    ("valija-bolso", "portafolio", ["otro"]),

    ("textil-hogar", "manteleria", ["mantel", "repasador", "servilleta", "otro"]),
]

PUBLICOS_EQUIVALENCIA = {
    "hombre": "hombre",
    "hombres": "hombre",
    "mujer": "mujer",
    "mujeres": "mujer",
    "niño": "nino",
    "niños": "nino",
    "nino": "nino",
    "ninos": "nino",
    "niña": "nino",
    "nina": "nino",
    "niñas": "nino",
    "ninas": "nino",
    "nino/a": "nino",
    "niño/a": "nino",
    "bebe": "nino",
    "bebe/a": "nino",
    "bebes": "nino",
    "bebe/s": "nino",
    "kid": "nino",
    "kids": "nino",
}


def calculate_execution_time(start_time, end_time):
    duration = end_time - start_time

    hours = int(duration // 3600)
    minutes = int((duration % 3600) // 60)
    seconds = duration % 60

    return f"{hours}h {minutes}m {seconds:.2f}s"


def build_full_handle(category_gral, category, category_by_id):
    handles = []
    current = category
    while current:
        handles.insert(0, current["handle"]["es"])  # prepend
        parent_id = current["parent"]
        current = category_by_id.get(parent_id) if parent_id else None
    handles.insert(0, category_gral)
    return ",".join(handles)


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


def calculate_price(price, promotional_price=None, rango_precio=None):
    """
    Calcula el precio aplicando los multiplicadores de los rangos.

    Args:
        price: Precio base
        promotional_price: Precio promocional (opcional)
        rango_precio: Lista de tuplas (inicio, fin, multiplicador) que definen los rangos

    Returns:
        float: Precio con el multiplicador aplicado según el rango correspondiente
    """
    try:
        # Usar el precio promocional si está disponible, de lo contrario usar el precio normal
        precio = float(promotional_price) if promotional_price is not None else float(price)

        # Usar los rangos proporcionados o los rangos por defecto
        rangos = rango_precio if rango_precio is not None else RANGOS_PRECIO

        # Buscar en qué rango cae el precio y aplicar el multiplicador correspondiente
        for inicio, fin, multiplicador in rangos:
            if inicio <= precio < fin:
                return round(precio * multiplicador, 2)

        # Si no está en ningún rango, devolver el precio sin cambios
        return precio

    except (TypeError, ValueError) as e:
        logger.error(f"Error al calcular el precio: {e}")
        return price  # Devolver el precio original en caso de error


def asignar_categoria_jerarquica(info_set):
    normalizados = [normalizar(palabra) for palabra in info_set]

    for categoria, subcategoria, subsubcategorias in CATEGORIES_TO_CREATE:
        cat_norm = normalizar(categoria)
        subcat_norm = normalizar(subcategoria)

        if cat_norm in normalizados:
            # Categoria encontrada, seguir con subcategoria
            if subcat_norm in normalizados:
                subsub_matches = []
                for ssc in subsubcategorias:
                    ssc_norm = normalizar(ssc)
                    for palabra in normalizados:
                        if palabra == ssc_norm or (palabra.startswith(ssc_norm) and len(palabra) - len(ssc_norm) <= 3):
                            subsub_matches.append(ssc)
                            break
                if subsub_matches:
                    return [categoria, subcategoria] + subsub_matches
                else:
                    # No subsub encontrada, devolver 'otro' si está definido
                    return [categoria, subcategoria, "otro" if "otro" in subsubcategorias else None]
            else:
                # Subcategoria no encontrada, pero ¿hay alguna que matchee parcialmente?
                if any(normalizar(sc) in normalizados for cat, sc, _ in CATEGORIES_TO_CREATE if normalizar(categoria) == normalizar(cat)):
                    continue  # Otras subcategorías posibles, seguir buscando
                return [categoria, None, None]

    return [None, None, None]


def create_tags(datos):
    datos_lower = {normalizar(item) for item in datos if isinstance(item, str)}

    tienda_id = next((item for item in datos_lower if item.isdigit() and len(item) >= 6), None)

    publico_objetivo = next((PUBLICOS_EQUIVALENCIA[item] for item in datos_lower if item in PUBLICOS_EQUIVALENCIA), None)

    if any(palabra in datos_lower for palabra in ["bazar", "bano", "cocina"]):
        categoria_general = "bazar"
    elif any(palabra in datos_lower for palabra in ["blanqueria", "dormitorio"]):
        categoria_general = "blanqueria"
    else: 
        categorias_generales = {"indumentaria", "perfumeria", "electronica", "valija bolso", "textil hogar"}
        categoria_general = next((item for item in datos_lower if item in categorias_generales), None)

    categoria_especifica = find_categoria_especifica(datos_lower, categoria_general)

    datos_lower.add(normalizar(categoria_general))
    datos_lower.add(normalizar(categoria_especifica))
    logger.info(f"Datos normalizados: {datos_lower}")

    categorias = asignar_categoria_jerarquica(datos_lower)
    categorias.append(tienda_id)
    categorias.append(publico_objetivo)

    return categorias

def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    texto = re.sub(r'[-/&_]', ' ', texto)  # reemplaza guiones y símbolos por espacio
    texto = re.sub(r'[^a-z0-9 ]', '', texto)  # elimina todo lo demás
    texto = re.sub(r'\s+', ' ', texto)  # colapsa espacios múltiples
    return texto.strip()


def find_categoria_especifica(datos_normalizados, categoria_general):
    if categoria_general not in TAGS_EQUIVALENCIA:
        return None

    equivalencias = TAGS_EQUIVALENCIA[categoria_general]

    # 1. Intento exacto
    for item in sorted(datos_normalizados):
        if item in equivalencias:
            return equivalencias[item]

    # 2. Intento por substring (más flexible)
    for item in sorted(datos_normalizados):
        for key in equivalencias:
            if key in item:
                return equivalencias[key]

    return "otro"
