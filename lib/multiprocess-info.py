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


def get_locations(initial_page):
    print('Initial_Page: {}'.format(initial_page))
    page = initial_page[0]
    max_page = initial_page[0] + initial_page[1]
    print('Max_Page: {}'.format(max_page))
    resp = oipa_rest_get_info.get_locs(page)
    print(resp.status_code)
    if resp.status_code != 200:
        raise ApiError(
            'No se pudo obtener publishers: {}'.format(resp.status_code))
    #loc_resp = resp.json()
    ##for part in loc_resp:
    ##    print('{}:{}'.format(part, loc_resp[part]))
    #while(loc_resp['next'] != None and page <= max_page):
    #    try:
    #        print('Page:{} | {}'.format(page, max_page-page))
    #        resp = oipa_rest_get_info.get_locs(page)
    #        loc_resp = resp.json()
    #        if(loc_resp['results'] == None):
    #            print(loc_resp)
    #        db = connectDB()
    #        locations = db.locations
    #        ids = locations.insert_many(loc_resp['results'])
    #        #print(ids.inserted_ids)
    #        
    #        page = page+1
    #    except Exception:
    #        print(traceback.format_exc())
    return ('Done {}'.format(initial_page))

def connectDB():
    client = MongoClient('localhost',27017)
    db = client.iati_tsne
    return db

def main():
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        pages = [(55702,3018),(59707,3061),(63709,3107),(67719,3145),(71722,3190),(75739,3221),(79758,3250),(83776,3281)]
        results = executor.map(get_locations, pages)

        for result in results:
            print(result)

    end = time.perf_counter()
    print('Done in {} seconds'.format(round(end-start, 2)))

if __name__ == '__main__':
	main()