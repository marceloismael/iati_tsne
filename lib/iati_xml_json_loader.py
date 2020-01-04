###Filename: oipa_rest_get_info.py
# Autor: Marcelo Carrillo
# Creado: 27/11/2019
# Modificado 27/11/2019
# Descripcion: Recorrer todos los directorios del IATI DataDump .
import os 
from pymongo import MongoClient
import time
from collections import OrderedDict
import xml.etree.ElementTree as ET
from xmljson import Abdera
from json import dumps
import json
import traceback

def connectDB():
    client = MongoClient('localhost',27017)
    db = client.iati_tsne
    return db

def process_file(dir, filename):
    try:
        #TODO: open file as string load xml tree set ordered dict using Abdera convention, transforme in JSON, obtain activities send to mongo
        fullname = dir + '/'+ filename
        #instanciar Abdera
        ab = Abdera(dict_type=OrderedDict)
        #Conexion a Mongo
        db = connectDB()
        #Carga de Archivo XML
        tree = ET.parse(fullname)
        root = tree.getroot()
        
        xmlstr = ET.tostring(root, encoding='unicode')
        xmlstr = xmlstr.replace("."," ")
        #Conversion a JSON de activities
        json_activity = dumps(ab.data(ET.fromstring(xmlstr)))
        json_activity = json_activity.replace("http://www.w3.org/XML/1998/namespace","")
        activities = json.loads(json_activity)
        activities_full = db.activities_full
        #Obtener lista de actividades y Enviar en grupo a la base
        ids = activities_full.insert_many(activities['iati-activities']['children'])
        print(ids.inserted_ids)
    except Exception:
        print(traceback.format_exc())

def main():
    start = time.perf_counter()
    try:
        rootDir = '/Users/marcelocarrillo/Documents/Master Big Data and Visual Analytics/TFM/IATI-TSNE/App/data/iati_data/data/'
        for dirName, subdirList, fileList in os.walk(rootDir, topdown=False):
            #print('Carpeta: %s' % dirName)
            for fname in fileList:
                #Validar si se trata de activity u organization
                if(("org" in fname) == False):
                    print('\t%s' % fname)
                    process_file(dirName, fname)
    except Exception:
        print(traceback.format_exc())
    end = time.perf_counter()
    print('Done in {} seconds'.format(round(end-start, 2)))

if __name__ == '__main__':
    main()
