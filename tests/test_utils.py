import os
import sys

from pytest import approx
from app.utils import calculate_price

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath('..'))

# Rangos de precios para pruebas
# Nota: Los rangos son [inicio, fin), fin no está incluido
RANGOS_PRUEBA = [
    (0, 9000.00, 1.35),  # Rango 1: 0.00 - 9000.00 (9000.00 no incluido)
    (9000.00, 20000.00, 1.28),  # Rango 2: 9000.00 - 20000.00
    (20000.00, 30000.00, 1.23),  # Rango 3: 20000.00 - 30000.00
    (30000.00, 40000.00, 1.19),  # Rango 4: 30000.00 - 40000.00
    (40000.00, 50000.00, 1.16),  # Rango 5: 40000.00 - 50000.00
    (50000.00, 60000.00, 1.14),  # Rango 6: 50000.00 - 60000.00
    (60000.00, 100000.00, 1.12),  # Rango 7: 60000.00 - 100000.00
    (100000.00, 200000.00, 1.1)  # Rango 8: 100000.00 - 200000.00
]

# Casos de prueba para rangos estándar
# Nota: Los rangos son [inicio, fin), es decir, fin no está incluido
CASOS_RANGOS = [
    (5000, None, 5000 * 1.35),      # Rango 1 (0.00 - 9000.00)
    (8999.99, None, 8999.99 * 1.35),  # Último valor del Rango 1
    (9000, None, 9000 * 1.28),       # Primer valor del Rango 2 (9000.00 - 20000.00)
    (15000, None, 15000 * 1.28),     # Medio Rango 2
    (199999.99, None, 199999.99 * 1.1),  # Último valor del Rango 8
    (200000, None, 200000),          # Fuera de rango

    # Casos adicionales para verificar los límites
    (0, None, 0),                    # Precio 0
    (1, None, 1.35),                 # Precio mínimo en el primer rango
    (8999.98, None, 8999.98 * 1.35),  # Casi al final del primer rango
    (9000.01, None, 9000.01 * 1.28),  # Justo después del límite del primer rango
    (199999.98, None, 199999.98 * 1.1),  # Casi al final del último rango
]


def test_rangos_precios():
    """Prueba los cálculos de precios en diferentes rangos."""
    for precio, promo, esperado in CASOS_RANGOS:
        print(f"\n--- Probando precio: {precio}, promocional: {promo} ---")
        print(f"Esperado: {esperado}")

        # Llamar a la función con los rangos de prueba
        resultado = calculate_price(precio, promo, RANGOS_PRUEBA)
        print(f"Resultado: {resultado}")

        # Mostrar los rangos que se están utilizando
        print("Rangos de prueba:")
        for i, (inicio, fin, mult) in enumerate(RANGOS_PRUEBA, 1):
            print(f"  Rango {i}: {inicio} <= x < {fin} (x{mult})")

        # Verificar el resultado
        assert resultado == approx(esperado, abs=1e-2), \
               f"Error en {precio}: esperado {esperado}, obtenido {resultado}"


def test_precio_promocional():
    """Prueba que el precio promocional tiene prioridad."""
    assert calculate_price(15000, 8000) == 8000 * 1.35
    assert calculate_price(15000, None) == 15000 * 1.28


def test_casos_especiales():
    """Prueba casos especiales como cero y valores negativos."""
    assert calculate_price(0, None) == 0
    assert calculate_price(-1000, None) == -1000
    assert calculate_price(200000, None) == 200000


def test_rangos_personalizados():
    """Prueba con rangos personalizados."""
    rangos = [(0, 100, 1.5), (100, 200, 1.25), (200, 300, 1.1)]
    casos = [
        (50, None, 75),     # 50 * 1.5 = 75
        (150, None, 187.5),  # 150 * 1.25 = 187.5
        (250, None, 275),   # 250 * 1.1 = 275
        (350, None, 350)    # Fuera de rango
    ]

    for precio, promo, esperado in casos:
        resultado = calculate_price(precio, promo, rangos)
        assert resultado == esperado, f"Error en {precio}: {resultado} != {esperado}"
