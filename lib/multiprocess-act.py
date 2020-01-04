# Filename: multiprocess_info.py
# Autor: Marcelo Carrillo
# Creado: 26/11/2019
# Modificado 26/11/2019
# Descripcion: Procesar la informacion de actividades, transacciones y ubicaciones del estandar IAPI y 
# enviarlas a la base de datos utilizando multiples procesadores.

import oipa_rest_get_info
import json
import time
from pymongo import MongoClient
import concurrent.futures
import traceback


def get_activities(initial_page):
    print('Initial Page: {}'.format(initial_page))
    page = initial_page
    max_page = initial_page + 1511
    print('Max Page: {}'.format(max_page))
    resp = oipa_rest_get_info.get_activities()
    print(resp.status_code)
    if resp.status_code != 200:
        raise ApiError(
            'No se pudo obtener activities: {}'.format(resp.status_code))
    act_resp = resp.json()
    #for part in act_resp:
    #    print('{}:{}'.format(part, act_resp[part]))
    while(act_resp['next'] != None and page <= max_page):
        try:
            print('Page:{}'.format(page))
            resp = oipa_rest_get_info.get_activities(page)
            act_resp = resp.json()
            if(act_resp['results'] == None):
                print(act_resp)
            db = connectDB()
            activities = db.activities
            ids = activities.insert_many(act_resp['results'])
            print(ids.inserted_ids)
            
            page = page+1
        except Exception:
            print(traceback.format_exc())
    return ('Done {}'.format(initial_page))

def connectDB():
    client = MongoClient('localhost',27017)
    db = client.iati_tsne
    return db

def main():
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        pages = [80102,81613,83124,84635,86146,87657,89168,90679,92190,93701,95212,96723,98234,99745,101256,102767,104278,105789,107300,108811]
        results = executor.map(get_activities, pages)

        for result in results:
            print(result)

    end = time.perf_counter()
    print('Done in {} seconds'.format(round(end-start, 2)))

if __name__ == '__main__':
	main()