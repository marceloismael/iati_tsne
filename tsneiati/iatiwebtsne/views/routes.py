# Filename: main.py
# Autor: Marcelo Carrillo
# Creado: 09/12/2019
# Modificado 01/01/2020
# Descripcion: Administra las rutas posibles de la aplicacion
from flask import Blueprint, render_template, request, session
from flask import current_app as app
import pandas as pd
import numpy as np
import json
import time
from sklearn.manifold import TSNE
from sklearn.preprocessing import LabelEncoder
#from flask import session
#from flask import jsonify
from flask_wtf import FlaskForm
from wtforms import BooleanField, SelectField, StringField, HiddenField
#from wtforms.validators import SelectRequired
import os


# Definicion de Blueprints de la aplicacion (Secciones)
mod = Blueprint('mod', __name__, template_folder='templates')
css = Blueprint('css', __name__, static_folder='static')


class Dimensions(FlaskForm):
    Currency = BooleanField(u'Moneda', id='Currency', default='checked')
    Language = BooleanField(u'Idioma', id='Language', default='checked')
    Humanitarian = BooleanField(
        u'Es humanitaria', id='Humanitarian', default='checked')
    Hierarchy = BooleanField(u'Jerarquia', id='Hierarchy', default='checked')
    ReferenceRepOrg = BooleanField(
        u'Referencia Org', id='ReferenceRepOrg', default='checked')
    TypeRepOrgCode = BooleanField(
        u'Tipo Org ', id='TypeRepOrgCode', default='checked')
    NameRepOrg = BooleanField(
        u'Nombre Org', id='NameRepOrg', default='checked')
    NWordsTitle = BooleanField(
        u'Longitud del Titulo', id='NWordsTitle', default='checked')
    TypeDescCode = BooleanField(
        u'Tipo de Descripcion', id='TypeDescCode', default='checked')
    NWordsDesc = BooleanField(u'Longitud Descripcion',
                              id='NWordsDesc', default='checked')
    PartOrgRefCode = BooleanField(
        u'Cod. Org. Participante', id='PartOrgRefCode', default='checked')
    PartOrgTypeCode = BooleanField(
        u'Tipo Org. Participante', id='PartOrgTypeCode', default='checked')
    PartOrgRoleCode = BooleanField(
        u'Cod. Rol Org. Participante', id='PartOrgRoleCode', default='checked')
    NumPartOrg = BooleanField(
        u'Num. Org. Participante', id='NumPartOrg', default='checked')
    ActivityStatusCode = BooleanField(
        u'Estado Actividad', id='ActivityStatusCode', default='checked')
    PlannedDuration = BooleanField(
        u'Duracion planificada', id='PlannedDuration', default='checked')
    ActuaDuration = BooleanField(
        u'Duracion real ', id='ActuaDuration', default='checked')
    Scope = BooleanField(u'Alcance', id='Scope', default='checked')
    RecipientCountry = BooleanField(
        u'Pais Beneficiario', id='RecipientCountry', default='checked')
    Location = BooleanField(u'Ubicacion', id='Location', default='checked')
    Latitude = BooleanField(u'Latitud', id='Latitude', default='checked')
    Longitude = BooleanField(u'Longitud', id='Longitude', default='checked')
    Designation = BooleanField(
        u'Designacion', id='Designation', default='checked')
    Sector = BooleanField(u'Cod Sector', id='Sector', default='checked')
    SectorName = BooleanField(
        u'Nombre Sector', id='SectorName', default='checked')
    HumanitarianScopeType = BooleanField(
        u'Tipo Alcance Humanitario', id='HumanitarianScopeType', default='checked')
    HumanitarianScopeCode = BooleanField(
        u'Cod. Alcance Humanitario', id='HumanitarianScopeCode', default='checked')
    NumPolicyMarkers = BooleanField(
        u'Numero de Marcadores de Politica', id='NumPolicyMarkers', default='checked')
    CollaborationType = BooleanField(
        u'Tipo de Colaboracion', id='CollaborationType', default='checked')
    FlowType = BooleanField(u'Tipo de Flujo', id='FlowType', default='checked')
    FinType = BooleanField(u'Tipo de Financiamiento',
                           id='FinType', default='checked')
    AidType = BooleanField(u'Tipo de Ayuda', id='AidType', default='checked')
    TiedStatus = BooleanField(u'Estado de Compromiso',
                              id='TiedStatus', default='checked')
    BudgetType = BooleanField(u'Tipo de Presupuesto',
                              id='BudgetType', default='checked')
    BudgetDayAvgValue = BooleanField(
        u'Presupuesto Promedio Diario', id='BudgetDayAvgValue', default='checked')
    DisbursementType = BooleanField(
        u'Tipo de Desembolso', id='DisbursementType', default='checked')
    DisbursementDayAvgValue = BooleanField(
        u'Desmbolso promedio diario', id='DisbursementDayAvgValue', default='checked')
    IncomingFunds = BooleanField(
        u'Fondos entrantes', id='IncomingFunds', default='checked')
    Commitments = BooleanField(
        u'Montos comprometidos', id='Commitments', default='checked')
    Disbursements = BooleanField(
        u'Desembolsos', id='Disbursements', default='checked')
    Expenditures = BooleanField(
        u'Gastos', id='Expenditures', default='checked')
    InterestsRepayments = BooleanField(
        u'Pago de Intereses', id='InterestsRepayments', default='checked')
    LoanRepayments = BooleanField(
        u'Pago de Prestamos', id='LoanRepayments', default='checked')
    Reimbursements = BooleanField(
        u'Reembolsos', id='Reimbursements', default='checked')
    PurchaseOfEquity = BooleanField(
        u'Compra Equidades', id='PurchaseOfEquity', default='checked')
    SaleOfEquity = BooleanField(
        u'Venta Equidades', id='SaleOfEquity', default='checked')
    CreditGuarantees = BooleanField(
        u'Garantias de Credito', id='CreditGuarantees', default='checked')
    IncomingCommitments = BooleanField(
        u'Compromisos Entrantes', id='IncomingCommitments', default='checked')
    Transactions = BooleanField(
        u'Num Transacciones', id='Transactions', default='checked')
    color = SelectField(u'Color por:', id='Color', choices=[('Currency', 'Moneda'), ('Language', 'Idioma'),
                                                            ('Humanitarian', 'Es humanitaria'), ('NameRepOrg', 'Nombre Org'), (
                                                                'PartOrgName', 'Nombre Org. Participante'),
                                                            ('RecipientCountry', 'Pais Beneficiario'), ('Location', 'Ubicacion'), (
                                                                'srsName', 'Cod. SRS'), ('Designation', 'Designacion'),
                                                            ('SectorName', 'Nombre Sector'), ('HumanitarianScopeName', 'Nombre Alcance Humanitario')])
    size = SelectField(u'Tamaño por:', id='Size', choices=[('NWordsTitle', 'Longitud del Titulo'),
                                                           ('NWordsDesc', 'Longitud Descripcion'), ('NumPartOrg', 'Num. Org. Participante'), (
                                                               'PlannedDuration', 'Duracion planificada'),
                                                           ('ActuaDuration', 'Duracion real '), ('NumPolicyMarkers',
                                                                                                 'Numero de Marcadores de Politica'),
                                                           ('BudgetDayAvgValue', 'Presupuesto Promedio Diario'), (
        'DisbursementDayAvgValue', 'Desembolso promedio diario'),
        ('IncomingFunds', 'Fondos entrantes'), ('Commitments',
                                                'Montos comprometidos'), ('Disbursements', 'Desembolsos'),
        ('Expenditures', 'Gastos'), ('InterestsRepayments',
                                     'Pago de Intereses'), ('LoanRepayments', 'Pago de Prestamos'),
        ('Reimbursements', 'Reembolsos'), ('PurchaseOfEquity',
                                           'Compra Equidades'), ('SaleOfEquity', 'Venta Equidades'),
        ('CreditGuarantees', 'Garantias de Credito'), ('IncomingCommitments',
                                                       'Compromisos Entrantes'),
        ('Transactions', 'Num Transacciones')])


@mod.route('/', methods=['GET', 'POST'])
def vizualization():
    # Instancia de Objeto Formulario
    form = Dimensions()
    if(form.validate_on_submit()):
        data = apply_tsne(form.data)
        return render_template('visualization.html', form=form, data=data)
    else:
        return render_template('visualization.html', form=form)


def apply_tsne(params):
    start = time.perf_counter()
    # Acceder a la Data deployada en el servidor web
    pickle = os.path.join(app.config['DATA_URL'], os.environ.get('PICKLE_NAME'))
    # Cargar el Dataframe desde el Pickle
    iati_full = pd.read_pickle(pickle)
    # Procesar objeto param que corresponde a los parametros del formulario web
    dimColor = params.get('color')
    dimSize = params.get('size')
    lstSelected = ['Identifier', 'NameRepOrg', 'Title',
                   'PlannedStartDate', 'PlannedEndDate', 'BudgetDayAvgValue']
    for fieldname, value in params.items():
        if(fieldname not in ['color', 'size', 'csrf_token']):
            if(value):
                if(fieldname not in lstSelected):
                    lstSelected.append(fieldname)
    # Subset de DataFrame
    iati = iati_full[lstSelected]
    # TODO: Con el dataframe con las columnas seleccionadas codificar las variables categoricas dentro de un pipeline
    lstCategorical = ['Currency', 'Language', 'Humanitarian', 'Hierarchy', 'ReferenceRepOrg', 'TypeRepOrgCode',
      'PartOrgRefCode', 'PartOrgTypeCode', 'PartOrgRoleCode', 'NumPartOrg', 'ActivityStatusCode',
      'Scope', 'RecipientCountry', 'Location', 
      'Sector', 'HumanitarianScopeCode', 'CollaborationType', 'FlowType', 'SectorName',
      'FinType', 'AidType', 'TiedStatus', 'BudgetType']
    subsetCat = []
    for col in lstCategorical:
        if(col in lstSelected):
            subsetCat.append(col) 
    num_dummies = len(subsetCat) 
    iati_cat = iati[subsetCat] 
    print('Columnas Categoricas:', iati_cat.shape)  
    if(num_dummies > 0):
        iati_cat = pd.get_dummies(iati_cat, columns = subsetCat)  
        iati_final =  pd.concat([iati,iati_cat],axis=1) 
        #print(iati_cat.dtypes) 
        iati = iati.drop(subsetCat,axis=1)
    else:
        iati_final = iati
    # Obtener solo las variables numericas
    numerics = ['uint8','int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df = iati_final.select_dtypes(include=numerics)
    print("Dataframe Dimensions", df.shape)
    # Transformar todos los numeros a flotantes
    df.astype('float32').dtypes
    # Convertir en matriz numérica
    X = df.to_numpy()
    # Instanciar el algoritmo t-SNE de scikit-learn
    model = TSNE(n_components=2, perplexity=50)
    # Se estima un modelo y lo aplicamos a los datos para reducir su dimensionalidad a 2 dimensiones
    emb_X = model.fit_transform(X)
    # Se crea un DataFrame
    iati_tsne = pd.DataFrame({'x': emb_X[:, 0], 'y': emb_X[:, 1]})
    # Se agrega columnas para escalas y tooltip
    # Escalas
    iati_tsne['Color'] = iati_full[dimColor]
    iati_tsne['Size'] = iati_full[dimSize]
    iati_tsne['Title'] = iati['Title']
    iati_tsne['NameRepOrg'] = iati['NameRepOrg']
    iati_tsne['Id'] = iati['Identifier']
    iati_tsne.reset_index()
    print(df.columns)
    end = time.perf_counter()
    print('Done in {} seconds'.format(round(end-start, 2)))
    return iati_tsne.to_json(orient="records")
