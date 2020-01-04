###Filename: oipa_rest_get_info.py
# Autor: Marcelo Carrillo
# Creado: 24/11/2019
# Modificado 24/11/2019
# Descripcion: Obtener la información de las distintas entidades del estándar IAPI a través del RESTful API provisto por OIPA.

import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
}

#def _URL(path, page=''):
#	if(page==''):
#		return 'https://149.210.216.21/api/' + path + '/?format=json'
#	else:
#		return 'https://149.210.216.21/api/' + path + '/?format=json&page=' + str(page) 

def _URL(path, page=''):
	if(page==''):
		return 'https://www.oipa.nl/api/' + path + '/?format=json'
	else:
		return 'https://www.oipa.nl/api/' + path + '/?format=json&page=' + str(page) 

def get_activities(next=''):
	if(next==''):
		return requests.get(_URL('activities')) 
	else:
		return requests.get(_URL(path='activities',page=next))

def get_results():
	if(next==''):
		return requests.get(_URL('results/aggregations'))
	else:
		return requests.get(_URL(path='results/aggregations',page=next))

def get_organisations(next=''):
	if(next==''):
		return requests.get(_URL('organisations')) 
	else:
		return requests.get(_URL(path='organisations',page=next))

def get_publishers(next=''):
	if(next==''):
		return requests.get(_URL('publishers'))
	else:
		return requests.get(_URL(path='publishers',page=next))      

def get_locs(next=''):
	if(next==''):
		return requests.get(_URL('locations'))
	else:
		print(_URL(path='locations',page=next))
		return requests.get(_URL(path='locations',page=next))

def get_cities(next=''):
	if(next==''):
		return requests.get(_URL('cities'))
	else:
		return requests.get(_URL(path='cities',page=next))  

def get_datasets(next=''):
	if(next==''):
		return requests.get(_URL('datasets'))
	else:
		return requests.get(_URL(path='datasets',page=next))  

def get_sectors(next=''):
	if(next==''):
		return requests.get(_URL('sectors')) 
	else:
		return requests.get(_URL(path='sectors',page=next))

def get_countries(next=''):
	if(next==''):
		return requests.get(_URL('countries'))
	else:
		return requests.get(_URL(path='countries',page=next)) 


def get_regions():
	if(next==''):
		return requests.get(_URL('regions'))
	else:
		return requests.get(_URL(path='regions',page=next))    

def get_budgets():
	if(next==''):
		return requests.get(_URL('budgets/aggregations'))
	else:
		return requests.get(_URL(path='budgets/aggregations',page=next))   

def get_codelists(path):
	return requests.get(_URL(path)) 

def get_chains():
	return requests.get(_URL('chains'))
	if(next==''):
		return requests.get(_URL('chains'))
	else:
		return requests.get(_URL(path='chains',page=next))

def get_transactions():
	if(next==''):
		return requests.get(_URL('transactions'))
	else:
		return requests.get(_URL(path='transactions',page=next))
