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
