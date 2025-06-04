import os
import logging

from datetime import datetime, timedelta


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
