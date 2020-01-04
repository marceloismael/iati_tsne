###Filename: process_info.py
# Autor: Marcelo Carrillo
# Creado: 24/11/2019
# Modificado 24/11/2019
# Descripcion: Procesar la información de las distintas entidades del estándar IAPI y enviarlas a la base de datos.

import oipa_rest_get_info
import json
from pymongo import MongoClient


def get_organisations():
	resp =  oipa_rest_get_info.get_organisations()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener organizaciones: {}'.format(resp.status_code))
	org_resp = resp.json()

	#for part in org_resp:
	#		print('{}:{}'.format(part, org_resp[part]))

	page = 2
	while(org_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_organisations(page)
			org_resp = resp.json()
			if(org_resp['results']==None):
				print(org_resp)
			db = connectDB()
			organisations = db.organisations
			ids = organisations.insert_many(org_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(org_resp)	

def get_publishers():
	resp =  oipa_rest_get_info.get_publishers()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener publishers: {}'.format(resp.status_code))
	pub_resp = resp.json()
	for part in pub_resp:
			print('{}:{}'.format(part, pub_resp[part]))
	page = 2
	while(pub_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_publishers(page)
			pub_resp = resp.json()
			if(pub_resp['results']==None):
				print(pub_resp)
			db = connectDB()
			publishers = db.publishers
			ids = publishers.insert_many(pub_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(pub_resp)	

def get_locations():
	resp =  oipa_rest_get_info.get_locs()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener publishers: {}'.format(resp.status_code))
	loc_resp = resp.json()
	#for part in loc_resp:
	#		print('{}:{}'.format(part, loc_resp[part]))
	page = 44648
	while(loc_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_locs(page)
			loc_resp = resp.json()
			if(loc_resp['results']==None):
				print(loc_resp)
			db = connectDB()
			locations = db.locations
			ids = locations.insert_many(loc_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(loc_resp)	


def get_cities():
	resp =  oipa_rest_get_info.get_cities()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener cities: {}'.format(resp.status_code))
	city_resp = resp.json()
	#for part in city_resp:
	#		print('{}:{}'.format(part, city_resp[part]))
	page = 2
	while(city_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_cities(page)
			city_resp = resp.json()
			if(city_resp['results']==None):
				print(city_resp)
			db = connectDB()
			cities = db.cities
			ids = cities.insert_many(city_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(city_resp)

def get_datasets():
	resp =  oipa_rest_get_info.get_datasets()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener datasets: {}'.format(resp.status_code))
	ds_resp = resp.json()
	#for part in ds_resp:
	#		print('{}:{}'.format(part, ds_resp[part]))
	page = 2
	while(ds_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_datasets(page)
			ds_resp = resp.json()
			if(ds_resp['results']==None):
				print(ds_resp)
			db = connectDB()
			datasets = db.datasets
			ids = datasets.insert_many(ds_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(ds_resp)	

def get_sectors():
	resp =  oipa_rest_get_info.get_sectors()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener sectores: {}'.format(resp.status_code))
	sec_resp = resp.json()
	for part in sec_resp:
			print('{}:{}'.format(part, sec_resp[part]))
	page = 2
	while(sec_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_sectors(page)
			sec_resp = resp.json()
			if(sec_resp['results']==None):
				print(sec_resp)
			db = connectDB()
			sectors = db.sectors
			ids = sectors.insert_many(sec_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(sec_resp)

def get_countries():
	resp =  oipa_rest_get_info.get_countries()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener countries: {}'.format(resp.status_code))
	cty_resp = resp.json()
	#for part in cty_resp:
	#		print('{}:{}'.format(part, cty_resp[part]))
	page = 2
	while(cty_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_countries(page)
			cty_resp = resp.json()
			if(cty_resp['results']==None):
				print(cty_resp)
			db = connectDB()
			countries = db.countries
			ids = countries.insert_many(cty_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(cty_resp)

def get_regions():
	resp =  oipa_rest_get_info.get_regions()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener regions: {}'.format(resp.status_code))
	reg_resp = resp.json()
	#for part in reg_resp:
	#		print('{}:{}'.format(part, reg_resp[part]))
	page = 2
	while(reg_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_regions(page)
			reg_resp = resp.json()
			if(reg_resp['results']==None):
				print(reg_resp)
			db = connectDB()
			regions = db.regions
			ids = regions.insert_many(reg_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(reg_resp)

def get_budgets():
	resp =  oipa_rest_get_info.get_budgets()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener budgets: {}'.format(resp.status_code))
	bgt_resp = resp.json()
	#for part in bgt_resp:
	#		print('{}:{}'.format(part, bgt_resp[part]))
	page = 2
	while(bgt_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_budgets(page)
			bgt_resp = resp.json()
			if(bgt_resp['results']==None):
				print(bgt_resp)
			db = connectDB()
			budgets = db.budgets
			ids = budgets.insert_many(bgt_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(bgt_resp)


def get_codelists():
	resp =  oipa_rest_get_info.get_codelists('codelists')
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener codelists: {}'.format(resp.status_code))
	cdl_resp = resp.json()
	for part in cdl_resp:
		print(part['name']+'\n')
		try:
			item = oipa_rest_get_info.get_codelists('codelists/{}'.format(part['name']))
			#codelist = '{'+'list:{}, values:{}'.format(part['name'],item.json())+'}'
			codelist = item.json()
			for elem in codelist:
				elem['list'] = part['name']
			db = connectDB()
			codelists = db.codelists
			ids = codelists.insert_many(codelist)
			print(ids.inserted_ids)
		except:
			print(part['name'])



def get_chains():
	resp =  oipa_rest_get_info.get_chains()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener chains: {}'.format(resp.status_code))
	chn_resp = resp.json()
	#for part in chn_resp:
	#		print('{}:{}'.format(part, chn_resp[part]))
	page = 2
	while(chn_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_chains(page)
			chn_resp = resp.json()
			if(chn_resp['results']==None):
				print(chn_resp)
			db = connectDB()
			chains = db.chains
			ids = codelists.insert_many(chn_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(chn_resp)

def get_transactions():
	resp =  oipa_rest_get_info.get_transactions()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener transactions: {}'.format(resp.status_code))
	tran_resp = resp.json()
	#for part in tran_resp:
	#		print('{}:{}'.format(part, tran_resp[part]))
	page = 2
	while(tran_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_transactions(page)
			tran_resp = resp.json()
			if(tran_resp['results']==None):
				print(tran_resp)
			db = connectDB()
			transactions = db.transactions
			ids = codelists.insert_many(tran_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(tran_resp)

def get_activities():
	resp =  oipa_rest_get_info.get_activities()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener organizaciones: {}'.format(resp.status_code))
	act_resp = resp.json()
	#for part in act_resp:
	#		print('{}:{}'.format(part, act_resp[part]))
	page = 61740
	while(act_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_activities(page)
			act_resp = resp.json()
			if(act_resp['results']==None):
				print(act_resp)
			db = connectDB()
			activities = db.activities
			ids = activities.insert_many(act_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(act_resp)


def get_results():
	resp =  oipa_rest_get_info.get_results()
	print(resp.status_code)
	if resp.status_code != 200:
		raise ApiError('No se pudo obtener results: {}'.format(resp.status_code))
	rslt_resp = resp.json()
	#for part in rslt_resp:
	#		print('{}:{}'.format(part, rslt_resp[part]))
	page = 2
	while(rslt_resp['next']!=None):
		try:
			print('Page:{}'.format(page))
			resp = oipa_rest_get_info.get_results(page)
			rslt_resp = resp.json()
			if(rslt_resp['results']==None):
				print(rslt_resp)
			db = connectDB()
			results = db.results
			ids = results.insert_many(rslt_resp['results'])
			print(ids.inserted_ids)
			#for result in org_resp['results']:
			#	print('{}\n{}'.format(result['organisation_identifier'],result))
			page=page+1
		except:
			print(rslt_resp)

def connectDB():
	client = MongoClient('localhost',27017)
	db = client.iati_tsne
	return db

def ApiError(msg):
	print(msg)
#get_organisations()
#get_publishers()

get_locations()
#get_cities()
#get_activities()

#get_datasets()
#get_sectors()
#get_countries()
#get_regions()
#get_transactions()
#get_codelists()




