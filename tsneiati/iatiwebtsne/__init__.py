# Filename: __init__.py
# Autor: Marcelo Carrillo
# Creado: 09/12/2019
# Modificado 09/12/2019
# Descripcion: Inicializa la aplicacion web

from flask import Flask
from iatiwebtsne.extensions import bootstrap
from iatiwebtsne.views.routes import mod
from iatiwebtsne.views.routes import css

#def create_app(config_object='iatiwebtsne.settings'):
app = Flask(__name__)

app.config.from_object('iatiwebtsne.settings')
bootstrap.init_app(app)
app.register_blueprint(mod)
app.register_blueprint(css)

#return app 
 
#if __name__ == '__main__': 
#    app = create_app()
#    app.run(debug=True) 