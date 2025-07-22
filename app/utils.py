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
