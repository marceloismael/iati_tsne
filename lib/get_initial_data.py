# Filename: get_initial_data.py
# Autor: Marcelo Carrillo
# Creado: 04/12/2019
# Modificado 02/01/2020
# Descripcion: Consulta todos los documentos de pertenecientes a los bancos de desarrollo y los transforma en un dataframe a nivel de proyecto.
import pymongo as pm
import pandas as pd
import bson
import time
import re
import traceback
import json
import requests
#from dateutil.relativedelta import *
#from dateutil.parser import *
from datetime import datetime, timedelta
from operator import itemgetter

# URL del OpenExchangeRates para obtener la conversion de monedas a Dolares.
CURRENCY_URL = 'https://openexchangerates.org/api/latest.json?app_id=dd078882b1434026babf4e0b561f4f31'
ALL_CURRENCY = requests.get(CURRENCY_URL).json()

def connectDB():
    client = pm.MongoClient('localhost', 27017)
    db = client.iati_tsne
    return db


def process(activities):
    # Defición de dataframe
    df = pd.DataFrame()
    # Listas para crear cada columna del conjunto de datos
    id = []
    act_updated = []
    act_lang = []
    act_currency = []
    humanitarian = []
    hierarchy = []
    identifier = []
    rep_org_ref = []
    rep_org_type = []
    rep_org_name = []
    title = []
    nwords_title = []
    type_desc = []
    nwords_desc = []
    porg_ref = []
    porg_type = []
    porg_role = []
    porg_name = []
    num_po = []
    act_status = []
    plan_start_dt = []
    act_start_dt = []
    plan_end_dt = []
    act_end_dt = []
    planned_dur = []
    actual_dur = []
    act_scope = []
    rec_country = []
    lst_loc_name = []
    lst_srs = []
    lst_latitude = []
    lst_longitude = []
    lst_designation = []
    lst_sector = []
    lst_sector_name = []
    lst_hs_type = []
    lst_hs_code = []
    lst_hs_name = []
    lst_policy_marker = []
    lst_collaboration_type = []
    lst_flow_type = []
    lst_fin_type = []
    lst_aid_type = []
    lst_tied_status = []
    lst_bgt_type = []
    lst_bgt_day_avg = []
    lst_bgt_currency = []
    lst_disb_type = []
    lst_disb_day_avg = []
    lst_disb_currency = []
    lst_inc_funds = []
    lst_commit = []
    lst_disbursements = []
    lst_expenditure = []
    lst_interest_repay = []
    lst_loan_repay = []
    lst_reimburse = []
    lst_purchase_eq = []
    lst_sale_eq = []
    lst_credit_gua = []
    lst_inc_commit = []
    lst_num_transactions = []
    # Transformar el cursor en lista y cargarlo completamente en memoria
    lact = list(activities)
    try:
        # Procesamiento de cada documento para extraer las dimensiones relevantes
        for doc in lact:
            # Diccionario de Control de Columnas para adicion de valores por defecto
            cd = {'id': 0, 'act_updated': 0, 'act_lang': 0, 'act_currency': 0, 'humanitarian': 0, 'hierarchy': 0, 'identifier': 0, 'rep_org_ref': 0, 'rep_org_type': 0,
                  'rep_org_name': 0, 'title': 0, 'nwords_title': 0, 'type_desc': 0, 'nwords_desc': 0, 'porg_ref': 0, 'porg_type': 0, 'porg_role': 0, 'porg_name': 0, 'num_po': 0,
                  'act_status': 0, 'plan_start_dt': 0, 'act_start_dt': 0, 'plan_end_dt': 0, 'act_end_dt': 0, 'planned_dur': 0, 'actual_dur': 0, 'act_scope': 0, 'rec_country': 0,
                  'lst_loc_name': 0, 'lst_srs': 0, 'lst_latitude': 0, 'lst_longitude': 0, 'lst_designation': 0, 'lst_sector': 0, 'lst_sector_name': 0, 'lst_hs_type': 0, 'lst_hs_code': 0,
                  'lst_hs_name': 0, 'lst_policy_marker': 0, 'lst_collaboration_type': 0, 'lst_flow_type': 0, 'lst_fin_type': 0, 'lst_aid_type': 0, 'lst_tied_status': 0, 'lst_bgt_type': 0,
                  'lst_bgt_day_avg': 0, 'lst_bgt_currency': 0, 'lst_disb_type': 0, 'lst_disb_day_avg': 0, 'lst_disb_currency': 0, 'lst_inc_funds': 0, 'lst_commit': 0, 'lst_disbursements': 0,
                  'lst_expenditure': 0, 'lst_interest_repay': 0, 'lst_loan_repay': 0, 'lst_reimburse': 0, 'lst_purchase_eq': 0, 'lst_sale_eq': 0, 'lst_credit_gua': 0, 'lst_inc_commit': 0,
                  'lst_num_transactions': 0}
            id.append(doc['_id'])
            cd['id'] = 1
            iati_activity = doc['iati-activity']
            act_attr = iati_activity['attributes']
            act_updated.append(act_attr['last-updated-datetime'])
            cd['act_updated'] = 1
            act_currency.append(act_attr['default-currency'])
            cd['act_currency'] = 1
            act_lang.append(act_attr['{}lang'])
            cd['act_lang'] = 1
            if('humanitarian' in act_attr):
                humanitarian.append(act_attr['humanitarian'])
            else:
                humanitarian.append(0)
            cd['humanitarian'] = 1
            if('hierarchy' in act_attr):
                hierarchy.append(act_attr['hierarchy'])
            else:
                hierarchy.append(0)
            cd['hierarchy'] = 1
            act_children = iati_activity['children']
            # Definir variable para obtener solo el primer participating-org
            first_part_org = True
            num_part_org = 0
            # Variables para fechas
            ps_dt = ""
            as_dt = ""
            pe_dt = ""
            ae_dt = ""
            num_dates = 0
            plan_duration = 0
            actual_duration = 0
            # Variables para Ubicacion
            loc_name = ""
            srs = ""
            latitude = 0
            longitude = 0
            designation = ""
            # Variable para solo el primer sector
            first_sector = True
            # Variable para solo el primer Recipient country
            first_rec_country = True
            # Variable para la primera descripcion
            first_desc = True
            # Variable para primer Activity scope
            first_act_scope = True
            # Variable para First Location
            first_loc = True
            # Variables para Humanitarian Scope
            hs_type = 0
            hs_code = 0
            hs_name = "Not Defined"
            # Conteo de Policy Markers
            num_policy_markers = 0
            # Acumuladores para Budget
            sum_bgt = 0
            sum_duration_bgt = 0
            first_bgt = True
            exist_bgt = False
            # Acumuladores para Planned Disbursement
            sum_disb = 0
            sum_duration_disb = 0
            first_disb = True
            exist_disb = False
            disb_type = 0
            disb_duration = 0
            disb_value = 0
            disb_currency = "USD"
            # Acumuladores para Transacciones
            sum_inc_funds = 0
            sum_commit = 0
            sum_disbursements = 0
            sum_expenditure = 0
            sum_interest_repay = 0
            sum_loan_repay = 0
            sum_reimburse = 0
            sum_purchase_eq = 0
            sum_sale_eq = 0
            sum_credit_gua = 0
            sum_inc_commit = 0
            num_transactions = 0
            lst_trans_typ = []
            # Identificar dentro de los hijos de cada documento su tipo y proceso adecuado
            for elem in act_children:
                if('iati-identifier' in elem):
                    iati_identifier = elem
                    identifier.append(iati_identifier['iati-identifier'])
                    cd['identifier'] = 1
                elif('reporting-org' in elem):
                    reporting_org = elem['reporting-org']
                    ref, type_rep_org, name_rep_org = process_reporting_org(
                        reporting_org)
                    rep_org_ref.append(ref)
                    cd['rep_org_ref'] = 1
                    rep_org_type.append(type_rep_org)
                    cd['rep_org_type'] = 1
                    rep_org_name.append(name_rep_org)
                    cd['rep_org_name'] = 1
                elif('title' in elem):
                    titulo = elem['title']
                    titl, nwor = process_title(titulo)
                    title.append(titl)
                    cd['title'] = 1
                    nwords_title.append(nwor)
                    cd['nwords_title'] = 1
                elif('description' in elem):
                    if(first_desc):
                        descripcion = elem['description']
                        typ_desc, nwor_desc = process_description(descripcion)
                        type_desc.append(typ_desc)
                        cd['type_desc'] = 1
                        nwords_desc.append(nwor_desc)
                        cd['nwords_desc'] = 1
                        first_desc = False
                elif('participating-org' in elem):
                    if(first_part_org):
                        part_org = elem['participating-org']
                        po_ref, po_type, po_role, po_name = process_part_org(
                            part_org)
                        porg_ref.append(po_ref)
                        cd['porg_ref'] = 1
                        porg_type.append(po_type)
                        cd['porg_type'] = 1
                        porg_role.append(po_role)
                        cd['porg_role'] = 1
                        porg_name.append(po_name)
                        cd['porg_name'] = 1
                        first_part_org = False
                    num_part_org = num_part_org + 1
                    cd['num_po'] = 1
                elif('activity-status' in elem):
                    act_status.append(
                        elem['activity-status']['attributes']['code'])
                    cd['act_status'] = 1
                elif('activity-date' in elem):
                    act_dt = elem['activity-date']
                    tipo = act_dt['attributes']['type']
                    fecha = act_dt['attributes']['iso-date']
                    if(tipo == 1):
                        ps_dt = fecha
                        plan_start_dt.append(ps_dt)
                        cd['plan_start_dt'] = 1
                        num_dates = num_dates + 1
                    if(tipo == 2):
                        as_dt = fecha
                        act_start_dt.append(as_dt)
                        cd['act_start_dt'] = 1
                        num_dates = num_dates + 1
                    if(tipo == 3):
                        pe_dt = fecha
                        plan_end_dt.append(pe_dt)
                        cd['plan_end_dt'] = 1
                        num_dates = num_dates + 1
                    if(tipo == 4):
                        ae_dt = fecha
                        act_end_dt.append(ae_dt)
                        cd['act_end_dt'] = 1
                        num_dates = num_dates + 1
                elif('activity-scope' in elem):
                    if(first_act_scope):
                        scope = elem['activity-scope']
                        scp = scope['attributes']['code']
                        act_scope.append(scp)
                        cd['act_scope'] = 1
                        first_act_scope = False
                elif ('recipient-country' in elem):
                    if(first_rec_country):
                        country = elem['recipient-country']
                        rec_country.append(country["attributes"]["code"])
                        cd['rec_country'] = 1
                        first_rec_country = False
                elif('location' in elem):
                    if(first_loc):
                        location = elem['location']
                        loc_name, srs, latitude, longitude, designation = process_location(
                            location)
                        lst_loc_name.append(loc_name)
                        cd['lst_loc_name'] = 1
                        lst_srs.append(srs)
                        cd['lst_srs'] = 1
                        lst_latitude.append(latitude)
                        cd['lst_latitude'] = 1
                        lst_longitude.append(longitude)
                        cd['lst_longitude'] = 1
                        lst_designation.append(designation)
                        cd['lst_designation'] = 1
                        first_loc = False
                elif('sector' in elem):
                    if(first_sector):
                        sector = elem['sector']
                        lst_sector.append(sector['attributes']['code'])
                        cd['lst_sector'] = 1
                        if("children" in sector):
                            lst_sector_name.append(
                                sector['children'][0]['narrative'])
                        else:
                            lst_sector_name.append("Not Defined")
                        cd['lst_sector_name'] = 1
                        first_sector = False
                elif('humanitarian-scope' in elem):
                    human_scope = elem['humanitarian-scope']
                    hs_type, hs_code, hs_name = process_humanitarian(
                        human_scope)
                    lst_hs_type.append(hs_type)
                    cd['lst_hs_type'] = 1
                    lst_hs_code.append(hs_code)
                    cd['lst_hs_type'] = 1
                    lst_hs_name.append(hs_name)
                    cd['lst_hs_type'] = 1
                elif('policy-marker' in elem):
                    num_policy_markers = num_policy_markers + 1
                    cd['lst_policy_marker'] = 1
                elif('collaboration-type' in elem):
                    lst_collaboration_type.append(
                        elem['collaboration-type']['attributes']['code'])
                    cd['lst_collaboration_type'] = 1
                elif('default-flow-type' in elem):
                    lst_flow_type.append(
                        elem['default-flow-type']['attributes']['code'])
                    cd['lst_flow_type'] = 1
                elif('default-finance-type' in elem):
                    lst_fin_type.append(
                        elem['default-finance-type']['attributes']['code'])
                    cd['lst_fin_type'] = 1
                elif('default-aid-type' in elem):
                    lst_aid_type.append(
                        elem['default-aid-type']['attributes']['code'])
                    cd['default-aid-type'] = 1
                elif('default-tied-status' in elem):
                    lst_tied_status.append(
                        elem['default-tied-status']['attributes']['code'])
                    cd['lst_tied_status'] = 1
                elif('budget' in elem):
                    exist_bgt = True
                    budget = elem['budget']
                    bgt_type, bgt_duration, bgt_value, bgt_currency = process_budget(
                        budget)
                    if(first_bgt):
                        sum_bgt = bgt_value
                        sum_duration_bgt = bgt_duration
                        first_bgt = False
                    else:
                        sum_bgt = sum_bgt + bgt_value
                        sum_duration_bgt = sum_duration_bgt + bgt_duration
                    cd['lst_bgt_type'] = 1
                    cd['lst_bgt_day_avg'] = 1
                    cd['lst_bgt_currency'] = 1
                elif('planned-disbursement' in elem):
                    exist_disb = True
                    disbursement = elem['planned-disbursement']
                    disb_type, disb_duration, disb_value, disb_currency = process_planned_disbursement(
                        disbursement)
                    if(first_disb):
                        sum_disb = disb_value
                        sum_duration_disb = disb_duration
                        first_disb = False
                    else:
                        sum_disb = sum_disb + disb_value
                        sum_duration_disb = sum_duration_disb + disb_duration
                    cd['lst_disb_type'] = 1
                    cd['lst_disb_day_avg'] = 1
                    cd['lst_disb_currency'] = 1
                elif('transaction' in elem):
                    transaction = elem['transaction']
                    tran_type = 0
                    tran_value = 0
                    for chd in transaction['children']:
                        if('transaction_type' in chd):
                            tran_type = chd['transaction-type']['attributes']['code']
                        if('value' in chd):
                            v = chd['value']
                            if('children' in v):
                                tran_value = float(
                                    str(v['children'][0]).replace(' ', '.'))
                    # Procesamiento de Acumuladores
                    if(tran_type == 1):  # Incoming Funds
                        sum_inc_funds = sum_inc_funds + tran_value
                    if(tran_type == 2):  # Commitment
                        sum_commit = sum_commit + tran_value
                    if(tran_type == 3):  # Disbursement
                        sum_disbursements = sum_disbursements + tran_value
                    if(tran_type == 4):  # Expenditure
                        sum_expenditure = sum_expenditure + tran_value
                    if(tran_type == 5):  # Interest Repayment
                        sum_interest_repay = sum_interest_repay + tran_value
                    if(tran_type == 6):  # Loan Repayment
                        sum_loan_repay = sum_loan_repay + tran_value
                    if(tran_type == 7):  # Reimbursement
                        sum_reimburse = sum_reimburse + tran_value
                    if(tran_type == 8):  # Purchase of Equity
                        sum_purchase_eq = sum_purchase_eq + tran_value
                    if(tran_type == 9):  # Sale of Equity
                        sum_sale_eq = sum_sale_eq + tran_value
                    if(tran_type == 10):  # Credt Guarantee
                        sum_credit_gua = sum_credit_gua + tran_value
                    if(tran_type == 10):  # Incoming Commitment
                        sum_inc_commit = sum_inc_commit + tran_value
                    num_transactions = num_transactions + 1
                    cd['lst_inc_funds'] = 1
                    cd['lst_commit'] = 1
                    cd['lst_disbursements'] = 1
                    cd['lst_expenditure'] = 1
                    cd['lst_interest_repay'] = 1
                    cd['lst_loan_repay'] = 1
                    cd['lst_reimburse'] = 1
                    cd['lst_purchase_eq'] = 1
                    cd['lst_sale_eq'] = 1
                    cd['lst_credit_gua'] = 1
                    cd['lst_inc_commit'] = 1
                    cd['lst_num_transactions'] = 1
                elif('contact-info' in elem):
                    continue
                elif('country-budget-items' in elem):
                    continue
                elif('capital-spend' in elem):
                    continue
                elif('document-link' in elem):
                    continue
                elif('related-activity' in elem):
                    continue
                elif('conditions' in elem):
                    continue
                elif('result' in elem):
                    continue
                elif('other-identifier' in elem):
                    continue
                elif('recipient-region' in elem):
                    continue
            # Procesamiento de duraciones de fechas y alineacion de columnas no incluidas
            if(num_dates == 4):
                    # Procesar duraciones simples
                plan_duration = abs((datetime.fromisoformat(
                    pe_dt) - datetime.fromisoformat(ps_dt))/timedelta(days=1))
                actual_duration = abs((datetime.fromisoformat(
                    ae_dt) - datetime.fromisoformat(as_dt))/timedelta(days=1))
                planned_dur.append(plan_duration)
                cd['planned_dur'] = 1
                actual_dur.append(actual_duration)
                cd['actual_dur'] = 1
            else:
                # Procesar duraciones complejas (calculos adicionales)
                if(ps_dt != "" and pe_dt == ""):
                    pe_dt = str(datetime.now().isoformat())
                    plan_end_dt.append(pe_dt)
                    cd['plan_end_dt'] = 1
                    plan_duration = abs((datetime.fromisoformat(
                        pe_dt) - datetime.fromisoformat(ps_dt))/timedelta(days=1))
                    planned_dur.append(plan_duration)
                    cd['planned_dur'] = 1
                elif((ps_dt == "" and pe_dt != "") or (ps_dt == "" and pe_dt == "")):
                    plan_start_dt.append(ps_dt)
                    cd['plan_start_dt'] = 1
                    plan_end_dt.append(pe_dt)
                    cd['plan_end_dt'] = 1
                    planned_dur.append(plan_duration)
                    cd['planned_dur'] = 1
                else:
                    plan_duration = abs((datetime.fromisoformat(
                        pe_dt) - datetime.fromisoformat(ps_dt))/timedelta(days=1))
                    planned_dur.append(plan_duration)
                    cd['planned_dur'] = 1
                if(as_dt != "" and ae_dt == ""):
                    ae_dt = str(datetime.now().isoformat())
                    act_end_dt.append(ae_dt)
                    cd['act_end_dt'] = 1
                    actual_duration = abs((datetime.fromisoformat(
                        ae_dt) - datetime.fromisoformat(as_dt))/timedelta(days=1))
                    actual_dur.append(actual_duration)
                    cd['actual_dur'] = 1
                elif((as_dt == "" and ae_dt != "") or (as_dt == "" and ae_dt == "")):
                    act_start_dt.append(as_dt)
                    cd['act_start_dt'] = 1
                    act_end_dt.append(ae_dt)
                    cd['act_end_dt'] = 1
                    actual_dur.append(actual_duration)
                    cd['actual_dur'] = 1
                else:
                    actual_duration = abs((datetime.fromisoformat(
                        ae_dt) - datetime.fromisoformat(as_dt))/timedelta(days=1))
                    actual_dur.append(actual_duration)
                    cd['actual_dur'] = 1
            num_po.append(num_part_org)
            cd['num_po'] = 1

            if (cd['lst_bgt_type'] == 1):
                # Carga de Valores Agrupados de Bgt
                lst_bgt_type.append(bgt_type)
                if(sum_duration_bgt == 0):
                    lst_bgt_day_avg.append(0)
                else:
                    lst_bgt_day_avg.append(sum_bgt/sum_duration_bgt)
                lst_bgt_currency.append(bgt_currency)

            # Carga de Valores Agrupados de Planned Disbursement
            if (cd['lst_disb_type'] == 1):
                lst_disb_type.append(disb_type)
                if(sum_duration_disb == 0):
                    lst_disb_day_avg.append(0)
                else:
                    lst_disb_day_avg.append(sum_disb/sum_duration_disb)
                lst_disb_currency.append(disb_currency)

            # Carga de valores agrupados de Transacciones
            lst_inc_funds.append(sum_inc_funds)
            cd['lst_inc_funds'] = 1
            lst_commit.append(sum_commit)
            cd['lst_commit'] = 1
            lst_disbursements.append(sum_disbursements)
            cd['lst_disbursements'] = 1
            lst_expenditure.append(sum_expenditure)
            cd['lst_expenditure'] = 1
            lst_interest_repay.append(sum_interest_repay)
            cd['lst_interest_repay'] = 1
            lst_loan_repay.append(sum_loan_repay)
            cd['lst_loan_repay'] = 1
            lst_reimburse.append(sum_reimburse)
            cd['lst_reimburse'] = 1
            lst_purchase_eq.append(sum_purchase_eq)
            cd['lst_purchase_eq'] = 1
            lst_sale_eq.append(sum_sale_eq)
            cd['lst_sale_eq'] = 1
            lst_credit_gua.append(sum_credit_gua)
            cd['lst_credit_gua'] = 1
            lst_inc_commit.append(sum_inc_commit)
            cd['lst_inc_commit'] = 1
            lst_num_transactions.append(num_transactions)
            cd['lst_num_transactions'] = 1
            lst_hs_type.append(hs_type)
            cd['lst_hs_type'] = 1
            lst_hs_code.append(hs_code)
            cd['lst_hs_code'] = 1
            lst_hs_name.append(hs_name)
            cd['lst_hs_name'] = 1

            lst_policy_marker.append(num_policy_markers)
            cd['lst_policy_marker'] = 1

            # Usar el Diccionario de control para adicionar los valores por defecto faltantes
            dictMissing = {k: v for (k, v) in cd.items() if v == 0}
            for k, v in dictMissing.items():
                if (k == 'title'):
                    title.append("Not Defined")
                if (k == 'nwords_title'):
                    nwords_title.append(0)
                if (k == 'type_desc'):
                    type_desc.append("Not Defined")
                if (k == 'nwords_desc'):
                    nwords_desc.append(0)
                if (k == 'porg_ref'):
                    porg_ref.append("Not Defined")
                if (k == 'porg_type'):
                    porg_type.append(0)
                if (k == 'porg_role'):
                    porg_role.append(0)
                if (k == 'porg_name'):
                    porg_name.append("Not Defined")
                if (k == 'num_po'):
                    num_po.append(0)
                if (k == 'act_status'):
                    act_status.append(0)
                if (k == 'plan_start_dt'):
                    plan_start_dt.append(ps_dt)
                if (k == 'act_start_dt'):
                    act_start_dt.append(as_dt)
                if (k == 'plan_end_dt'):
                    plan_end_dt.append(pe_dt)
                if (k == 'act_end_dt'):
                    act_end_dt.append(ae_dt)
                if (k == 'planned_dur'):
                    planned_dur.append(plan_duration)
                if (k == 'actual_dur'):
                    actual_dur.append(actual_duration)
                if (k == 'act_scope'):
                    act_scope.append(0)
                if (k == 'rec_country'):
                    rec_country.append("Not Defined")
                if (k == 'lst_loc_name'):
                    lst_loc_name.append("Unspecified")
                if (k == 'lst_srs'):
                    lst_srs.append("Not Defined")
                if (k == 'lst_latitude'):
                    lst_latitude.append(latitude)
                if (k == 'lst_longitude'):
                    lst_longitude.append(longitude)
                if (k == 'lst_designation'):
                    lst_designation.append("Not Defined")
                if (k == 'lst_sector'):
                    lst_sector.append(0)
                if (k == 'lst_sector_name'):
                    lst_sector_name.append("Not Defined")
                if (k == 'lst_hs_type'):
                    lst_hs_type.append(0)
                if (k == 'lst_hs_code'):
                    lst_hs_code.append(0)
                if (k == 'lst_hs_name'):
                    lst_hs_name.append("Not Defined")
                if (k == 'lst_policy_marker'):
                    lst_policy_marker.append(0)
                if (k == 'lst_collaboration_type'):
                    lst_collaboration_type.append(0)
                if (k == 'lst_flow_type'):
                    lst_flow_type.append(0)
                if (k == 'lst_fin_type'):
                    lst_fin_type.append(0)
                if (k == 'lst_aid_type'):
                    lst_aid_type.append(0)
                if (k == 'lst_tied_status'):
                    lst_tied_status.append(0)
                if (k == 'lst_bgt_type'):
                    lst_bgt_type.append(0)
                if (k == 'lst_bgt_day_avg'):
                    lst_bgt_day_avg.append(0)
                if (k == 'lst_bgt_currency'):
                    lst_bgt_currency.append("USD")
                if (k == 'lst_disb_type'):
                    lst_disb_type.append(0)
                if (k == 'lst_disb_day_avg'):
                    lst_disb_day_avg.append(0)
                if (k == 'lst_disb_currency'):
                    lst_disb_currency.append("USD")
                if (k == 'lst_inc_funds'):
                    lst_inc_funds.append(0)
                if (k == 'lst_commit'):
                    lst_commit.append(0)
                if (k == 'lst_disbursements'):
                    lst_disbursements.append(0)
                if (k == 'lst_expenditure'):
                    lst_expenditure.append(0)
                if (k == 'lst_interest_repay'):
                    lst_interest_repay.append(0)
                if (k == 'lst_loan_repay'):
                    lst_loan_repay.append(0)
                if (k == 'lst_reimburse'):
                    lst_reimburse.append(0)
                if (k == 'lst_purchase_eq'):
                    lst_purchase_eq.append(0)
                if (k == 'lst_sale_eq'):
                    lst_sale_eq.append(0)
                if (k == 'lst_credit_gua'):
                    lst_credit_gua.append(0)
                if (k == 'lst_inc_commit'):
                    lst_inc_commit.append(0)
                if (k == 'lst_num_transactions'):
                    lst_num_transactions.append(0)

        print('id: ', len(id))
        print('act_updated: ', len(act_updated))
        print('act_lang: ', len(act_lang))
        print('act_currency: ', len(act_currency))
        print('humanitarian: ', len(humanitarian))
        print('hierarchy: ', len(hierarchy))
        print('identifier: ', len(identifier))
        print('rep_org_ref: ', len(rep_org_ref))
        print('rep_org_type: ', len(rep_org_type))
        print('rep_org_name: ', len(rep_org_name))
        print('title: ', len(title))
        print('nwords_title: ', len(nwords_title))
        print('type_desc: ', len(type_desc))
        print('nwords_desc: ', len(nwords_desc))
        print('porg_ref: ', len(porg_ref))
        print('porg_type: ', len(porg_type))
        print('porg_role: ', len(porg_role))
        print('porg_name: ', len(porg_name))
        print('num_po: ', len(num_po))
        print('act_status: ', len(act_status))
        print('plan_start_dt: ', len(plan_start_dt))
        print('act_start_dt: ', len(act_start_dt))
        print('plan_end_dt: ', len(plan_end_dt))
        print('act_end_dt: ', len(act_end_dt))
        print('planned_dur: ', len(planned_dur))
        print('actual_dur: ', len(actual_dur))
        print('act_scope: ', len(act_scope))
        print('rec_country: ', len(rec_country))
        print('lst_loc_name: ', len(lst_loc_name))
        print('lst_srs: ', len(lst_srs))
        print('lst_latitude: ', len(lst_latitude))
        print('lst_longitude: ', len(lst_longitude))
        print('lst_designation: ', len(lst_designation))
        print('lst_sector: ', len(lst_sector))
        print('lst_sector_name: ', len(lst_sector_name))
        print('lst_hs_type: ', len(lst_hs_type))
        print('lst_hs_code: ', len(lst_hs_code))
        print('lst_hs_name: ', len(lst_hs_name))
        print('lst_policy_marker: ', len(lst_policy_marker))
        print('lst_collaboration_type: ', len(lst_collaboration_type))
        print('lst_flow_type: ', len(lst_flow_type))
        print('lst_fin_type: ', len(lst_fin_type))
        print('lst_aid_type: ', len(lst_aid_type))
        print('lst_tied_status: ', len(lst_tied_status))
        print('lst_bgt_type: ', len(lst_bgt_type))
        print('lst_bgt_day_avg: ', len(lst_bgt_day_avg))
        print('lst_bgt_currency: ', len(lst_bgt_currency))
        print('lst_disb_type: ', len(lst_disb_type))
        print('lst_disb_day_avg: ', len(lst_disb_day_avg))
        print('lst_disb_currency: ', len(lst_disb_currency))
        print('lst_inc_funds: ', len(lst_inc_funds))
        print('lst_commit: ', len(lst_commit))
        print('lst_disbursements ', len(lst_disbursements))
        print('lst_expenditure: ', len(lst_expenditure))
        print('lst_interest_repay: ', len(lst_interest_repay))
        print('lst_loan_repay: ', len(lst_loan_repay))
        print('lst_reimburse: ', len(lst_reimburse))
        print('lst_purchase_eq: ', len(lst_purchase_eq))
        print('lst_sale_eq: ', len(lst_sale_eq))
        print('lst_credit_gua: ', len(lst_credit_gua))
        print('lst_inc_commit: ', len(lst_inc_commit))
        print('lst_num_transactions: ', len(lst_num_transactions))
        lot = list(zip(identifier, act_updated, act_currency, act_lang, humanitarian, hierarchy, rep_org_ref, rep_org_type,
                       rep_org_name, title, nwords_title, type_desc, nwords_desc, porg_ref, porg_type, porg_role, porg_name, num_po,
                       act_status, plan_start_dt, act_start_dt, plan_end_dt, act_end_dt, planned_dur, actual_dur, act_scope, rec_country,
                       lst_loc_name, lst_srs, lst_latitude, lst_longitude, lst_designation, lst_sector, lst_sector_name, lst_hs_type,
                       lst_hs_code, lst_hs_name, lst_policy_marker, lst_collaboration_type, lst_flow_type, lst_fin_type, lst_aid_type,
                       lst_tied_status, lst_bgt_currency, lst_bgt_type, lst_bgt_day_avg, lst_disb_currency, lst_disb_type, lst_disb_day_avg,
                       lst_inc_funds, lst_commit, lst_disbursements, lst_expenditure, lst_interest_repay, lst_loan_repay, lst_reimburse,
                       lst_purchase_eq, lst_sale_eq, lst_credit_gua, lst_inc_commit, lst_num_transactions))
        df = pd.DataFrame(lot, columns=['Identifier', 'Updated', 'Currency', 'Language', 'Humanitarian', 'Hierarchy',
                                        'ReferenceRepOrg', 'TypeRepOrgCode', 'NameRepOrg', 'Title', 'NWordsTitle', 'TypeDescCode', 'NWordsDesc', 'PartOrgRefCode',
                                        'PartOrgTypeCode', 'PartOrgRoleCode', 'PartOrgName', 'NumPartOrg', 'ActivityStatusCode',
                                        'PlannedStartDate', 'ActualStartDate', 'PlannedEndDate', 'ActualEndDate', 'PlannedDuration', 'ActuaDuration', 'Scope', 'RecipientCountry',
                                        'Location', 'srsName', 'Latitude', 'Longitude', 'Designation', 'Sector', 'SectorName', 'HumanitarianScopeType', 'HumanitarianScopeCode',
                                        'HumanitarianScopeName', 'NumPolicyMarkers', 'CollaborationType', 'FlowType', 'FinType', 'AidType', 'TiedStatus',
                                        'BudgetCurrency', 'BudgetType', 'BudgetDayAvgValue', 'DisbursementCurrency', 'DisbursementType', 'DisbursementDayAvgValue',
                                        'IncomingFunds', 'Commitments', 'Disbursements', 'Expenditures', 'InterestsRepayments', 'LoanRepayments', 'Reimbursements',
                                        'PurchaseOfEquity', 'SaleOfEquity', 'CreditGuarantees', 'IncomingCommitments', 'Transactions'])
        df.dtypes
    except Exception:
        print(traceback.format_exc())
    return df


def process_reporting_org(rep_org):
    ref_rep = ""
    type_rep = 0
    name_rep_org = ""
    rep_org_attr = rep_org['attributes']
    ref_rep = rep_org_attr['ref']
    type_rep = rep_org_attr['type']
    rep_org_children = rep_org['children']
    if('narrative' in rep_org_children[0]):
        son = rep_org_children[0]
        if('attributes' in son['narrative']):
            name_rep_org = son['narrative']['children'][0]
        else:
            name_rep_org = son['narrative']
    return ref_rep, type_rep, name_rep_org


def process_title(title):
    tit = ""
    nwor = 0
    if('narrative' in title):
        narrative = title['narrative']
        if('children' in narrative):
            if(isinstance(narrative, str)):
                tit = narrative
            else:
                tit = title['narrative']['children'][0]
        else:
            if(isinstance(narrative, str)):
                tit = narrative
    else:
        tit = title['children'][0]['narrative']['children'][0]
    nwor = nwords(tit)
    return tit, nwor


def process_description(desc):
    typ = ""
    nwor = 0
    try:
        if('attributes' in desc):
            typ = desc['attributes']['type']
        else:
            typ = 0
        if('children' in desc):
            narrative = desc['children'][0]['narrative']
            if(isinstance(narrative, str)):
                description = narrative
            else:
                if('children' in narrative):
                    description = narrative['children'][0]
                else:
                    description = ""
            nwor = nwords(description)
        else:
            nwor = 0
    except Exception:
        print(traceback.format_exc())
    return typ, nwor


def process_part_org(po):
    po_ref = ""
    po_name = ""
    po_type = 0
    po_role = 0
    try:
        if('attributes' in po):
            attr = po['attributes']
            if('ref' in attr):
                po_ref = attr['ref']
            if('type' in attr):
                po_type = attr['type']
            else:
                if('ref' in attr):
                    ref = attr['ref'].split('-')
                    po_type = ref[0]
                    po_ref = ref[1]
            po_role = attr['role']
        if('children' in po):
            child = po['children'][0]
            po_name = child['narrative']
    except Exception:
        print(traceback.format_exc())
    return po_ref, po_type, po_role, po_name


def process_location(location):
    loc_name = ""
    srs = ""
    latitude = 0
    longitude = 0
    designation = ""
    for child in location['children']:
        if('name' in child):
            loc_name = str(child['name']['narrative']).replace(',',' ')
        elif('point' in child):
            srs = (child['point']['attributes']['srsName']).replace(' ', '.')
            coor = str(child['point']['children'][0]['pos']).split(' ')
            if(len(coor) == 4):
                latitude = float(coor[0]+'.'+coor[1])
                longitude = float(coor[2]+'.'+coor[3])
            elif(len(coor) == 2):
                if(coor[0] != 'null' and coor[1] != 'null'):
                    latitude = float(coor[0])
                    longitude = float(coor[1])
        elif('feature-designation' in child):
            designation = child['feature-designation']['attributes']['code']
    return loc_name, srs, latitude, longitude, designation


def process_humanitarian(human_scope):
    hs_type = 0
    hs_code = 0
    hs_name = "Not Defined"
    hs_type = human_scope['attibutes']['type']
    hs_code = human_scope['attibutes']['code']
    if('children' in human_scope):
        hs_name = human_scope['children'][0]['narrative']
    return hs_type, hs_code, hs_name


def process_budget(budget):
    # print(budget)
    bgt_type = 0
    bgt_duration = 0
    bgt_currency = "USD"
    bgt_value = 0
    ps = ""
    pe = ""
    if('attributes' in budget):
        bgt_type = budget['attributes']['type']
    for child in budget['children']:
        if('period-start' in child):
            ps = child['period-start']['attributes']['iso-date']
        if('period-end' in child):
            pe = child['period-end']['attributes']['iso-date']
        if('value' in child):
            val = child['value']
            if('currency' in val['attributes']):
                bgt_currency = val['attributes']['currency']
            bgt_value = float(str(val['children'][0]).replace(' ', '.'))
    bgt_duration = float(
        abs((datetime.fromisoformat(pe)-datetime.fromisoformat(ps))/timedelta(days=1)))
    # print(bgt_value)
    # print(bgt_currency)
    return bgt_type, bgt_duration, bgt_value, bgt_currency


def process_planned_disbursement(disbursement):
    # print(disbursement)
    disb_type = 0
    disb_duration = 0
    disb_currency = "USD"
    disb_value = 0
    ps = ""
    pe = ""
    if('attributes' in disbursement):
        disb_type = disbursement['attributes']['type']
    for child in disbursement['children']:
        if('period-start' in child):
            ps = child['period-start']['attributes']['iso-date']
        if('period-end' in child):
            pe = child['period-end']['attributes']['iso-date']
        if('value' in child):
            val = child['value']
            if('currency' in val['attributes']):
                disb_currency = val['attributes']['currency']
            disb_value = float(str(val['children'][0]).replace(' ', '.'))
    disb_duration = float(
        abs((datetime.fromisoformat(pe)-datetime.fromisoformat(ps))/timedelta(days=1)))
    return disb_type, disb_duration, disb_value, disb_currency


def nwords(text):
    return len(re.findall(r'\w+', text))


def get_rate(frm):
    parsed = ALL_CURRENCY.get('rates')
    frm_rate = parsed.get(frm.upper())
    return 1/frm_rate


def main():
    start = time.perf_counter()
    
    lstBanks =['African Development Bank Group',
    'The Export-Import Bank of Korea(Economic Development Cooperation Fund)',
    'Asian Development Bank','Inter-American Development Bank','European Bank for Reconstruction and Development']
    df = pd.DataFrame(columns=['Identifier','Updated','Currency','Language','Humanitarian','Hierarchy', 'ReferenceRepOrg', 'TypeRepOrgCode', 'NameRepOrg', 'Title',
    'NWordsTitle',  'TypeDescCode', 'NWordsDesc', 'PartOrgRefCode', 'PartOrgTypeCode',  'PartOrgRoleCode',  'PartOrgName',  'NumPartOrg', 'ActivityStatusCode',
     'PlannedStartDate', 'ActualStartDate',  'PlannedEndDate', 'ActualEndDate',  'PlannedDuration',  'ActuaDuration',  'Scope',  'RecipientCountry', 'Location',
      'srsName',  'Latitude', 'Longitude',  'Designation',  'Sector', 'SectorName', 'HumanitarianScopeType',  'HumanitarianScopeCode',  'HumanitarianScopeName',
        'NumPolicyMarkers', 'CollaborationType',  'FlowType', 'FinType',  'AidType',  'TiedStatus', 'BudgetCurrency', 'BudgetType', 'BudgetDayAvgValue',
          'DisbursementCurrency', 'DisbursementType', 'DisbursementDayAvgValue',  'IncomingFunds',  'Commitments',  'Disbursements',  'Expenditures',
           'InterestsRepayments',  'LoanRepayments', 'Reimbursements', 'PurchaseOfEquity', 'SaleOfEquity', 'CreditGuarantees', 'IncomingCommitments',
             'Transactions', 'USDRate'])
    #Obtener muestra de cada Banco
    for bank in lstBanks:
        df = df.append(process_organization(bank),ignore_index=True)
    # Persistencia de DataFrame como Pickle
    df.to_csv("../tsneiati/iatiwebtsne/data/iati-tsne-pickle.csv")
    df.to_pickle(
        "../tsneiati/iatiwebtsne/data/iati-tsne-pickle.bz2", compression="infer", protocol=4)
    iati = pd.read_pickle("../tsneiati/iatiwebtsne/data/iati-tsne-pickle.bz2")
    print(iati)
    end = time.perf_counter()
    print('Done in {} seconds'.format(round(end-start, 2)))

def process_organization(organization):
    # Conexion a Mongo
    
    db = connectDB()
    activities_full = db.activities_full
    # Definición de Query
    activities = activities_full.find({"iati-activity.children.iati-identifier": {'$ne': ""},
                                       #"iati-activity.children.iati-identifier": {'$eq':"46002-G-Z1-KZ0-ZZZ-053"},
                                       #"iati-activity.children.reporting-org.children.narrative": {"$regex": ".*Bank", "$options": 'i'}
                                       "iati-activity.children.reporting-org.children.narrative": {"$in": [organization]}
                                       }).limit(300)
    print("Documentos que cumplen con el criterio de búsqueda: " + "500")
          #str(activities.collection.count_documents({"iati-activity.children.iati-identifier": {'$ne': ""},
          #                                           #"iati-activity.children.iati-identifier": {'$eq':"46002-G-Z1-KZ0-ZZZ-053"},
          #                                           #"iati-activity.children.reporting-org.children.narrative": {"$regex": ".*Bank", "$options": 'i'}
          #                                           "iati-activity.children.reporting-org.children.narrative": {"$in": ['Inter-American Development Bank']}
          #                                           }))
    # Transformacion de colección de documentos en Dataframe
    df = process(activities)

    # TODO: Adicionar columnas informativas con valores categoricos de codelists, columnas de presencia o no de secciones en doc

    # TODO: Unificar valores monetarios hacia USD utilizando algun tipo de OpenCurrency API
    rXDR = 0.0
    rKRW = 0.0
    rEUR = 0.0
    rUSD = 0.0
    currencies = ['XDR', 'KRW', 'EUR', 'USD']
    for cur in currencies:
        print(cur)
        if(cur == 'XDR'):
            rXDR = get_rate(cur)
            print(rXDR)
        if(cur == 'KRW'):
            rKRW = get_rate(cur)
            print(rKRW)
        if(cur == 'EUR'):
            rEUR = get_rate(cur)
            print(rEUR)
        if(cur == 'USD'):
            rUSD = get_rate(cur)
            print(rUSD)
    level_map = {'XDR': rXDR, 'KRW': rKRW, 'EUR': rEUR, 'USD': rUSD}
    df['USDRate'] = df['Currency'].map(level_map)
    # Reemplazo de columnas con montos en otras monedas con dolares.
    df['DisbursementDayAvgValue'] = df['DisbursementDayAvgValue'] * df['USDRate']
    df['IncomingFunds'] = df['IncomingFunds'] * df['USDRate']
    df['Commitments'] = df['Commitments'] * df['USDRate']
    df['Disbursements'] = df['Disbursements'] * df['USDRate']
    df['Expenditures'] = df['Expenditures'] * df['USDRate']
    df['InterestsRepayments'] = df['InterestsRepayments'] * df['USDRate']
    df['LoanRepayments'] = df['LoanRepayments'] * df['USDRate']
    df['Reimbursements'] = df['Reimbursements'] * df['USDRate']
    df['PurchaseOfEquity'] = df['PurchaseOfEquity'] * df['USDRate']
    df['SaleOfEquity'] = df['SaleOfEquity'] * df['USDRate']
    df['CreditGuarantees'] = df['CreditGuarantees'] * df['USDRate']
    df['IncomingCommitments'] = df['IncomingCommitments'] * df['USDRate']

    return df

    


if __name__ == '__main__':
    main()