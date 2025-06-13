import re
import unicodedata

from app.logger import logger

RANGOS_PRECIO = [
    (0.00, 9000.00, 1.35),
    (9000.00, 10000.00, 1.28),
    (20000.00, 30000.00, 1.23),
    (30000.00, 40000.00, 1.19),
    (40000.00, 50000.00, 1.16),
    (50000.00, 60000.00, 1.14),
    (60000.00, 90000.00, 1.12),
    (100000.00, 199000.01, 1.1),
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
        "repasador": "repasador",
        "repasadores": "repasador",

        "servilleta": "servilleta",
        "servilletas": "servilleta",

        "mantel": "mantel",
        "manteles": "mantel",
        "manteleria": "mantel",
        "mantelerias": "mantel",
        "individual": "mantel",
        "individuales": "mantel",
        "camino de mesa": "mantel",
        "caminos de mesa": "mantel",

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
    }
}

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


def calculate_price(price, promotional_price, rango_precio=None):
    precio = promotional_price if promotional_price else price

    if not rango_precio:
        rango_precio = RANGOS_PRECIO

    try:
        precio = float(precio)
    except ValueError:
        logger.error(f"Invalid price format: {precio}")

    for inicio, fin, procentaje in rango_precio:
        if inicio <= precio < fin:
            return precio * procentaje

    return precio


def create_tags(datos):
    datos_lower = {normalizar(item) for item in datos if isinstance(item, str)}
    logger.info(f"Datos normalizados: {datos_lower}")

    tienda_id = next((item for item in datos_lower if item.isdigit() and len(item) >= 6), None)

    publico_objetivo = next((PUBLICOS_EQUIVALENCIA[item] for item in datos_lower if item in PUBLICOS_EQUIVALENCIA), None)

    categorias_generales = {"indumentaria", "perfumeria", "tecnologia", "electronica", "bazar"}
    categoria_general = next((item for item in datos_lower if item in categorias_generales), None)

    categoria_especifica = find_categoria_especifica(datos_lower, categoria_general)

    return [categoria_especifica, tienda_id, publico_objetivo, categoria_general]


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
