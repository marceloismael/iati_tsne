# Filename: settings.py
# Autor: Marcelo Carrillo
# Creado: 09/12/2019
# Modificado 09/12/2019
# Descripcion: Administra los valores de configuracion

import os

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
print(APP_ROOT)
DATA_URL = APP_ROOT + '/data/' 
print(DATA_URL)
SECRET_KEY = 'SuperSecretKey'