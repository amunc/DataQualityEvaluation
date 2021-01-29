# -*- coding: utf-8 -*-
"""
Created on Mon May 27 14:30:27 2019

@author: Enrique
"""

from __future__ import division
import sys
import base64
import os
import gc
import codecs
import configparser as conp
import glob
import unicodedata
import copy
import csv
from fractions import Fraction
import scipy.sparse.linalg as sc
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import xhtml2pdf.pisa as pisa
from PyPDF2 import PdfFileMerger



# Definicion ruta y ficheros de trabajo:
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_SOURCE_CONFIG_FILE = 'data_source.ini'
EVENT_TYPOLOGY_CONFIG_FILE = 'event_typology.ini'
ENCODING = 'utf-8'

if os.name == 'nt':
    CONFIG_DIR = 'config\\'
    INPUT_DIR = 'input\\'
    OUTPUT_DIR = 'output\\'
    DUPLICATE_DIR = 'duplicate\\'
    TEMP_DIR = os.path.join(BASE_PATH, 'temp')
    TEMPORAL_PATH = os.path.join(BASE_PATH, 'temp\\')
else:
    CONFIG_DIR = 'config/'
    INPUT_DIR = 'input/'
    OUTPUT_DIR = 'output/'
    DUPLICATE_DIR = 'duplicate/'
    TEMP_DIR = os.path.join(BASE_PATH, 'temp')
    TEMPORAL_PATH = os.path.join(BASE_PATH, 'temp/')


# Numero maximo de lineas a tratar en cada iteracion:
CHUNKSIZE = 800000


# Campos base de datos:
FIELD_TYPOLOGY = 'EventTypology'
FIELD_DATA_SOURCE = 'DataSource'
FIELD_FIABILITY = 'Veracity'
FIELD_SEVERITY = 'Relevance'
FIELD_ID = 'Id'


# Campos para el procesado de la muestra de datos
SORT_FIELDS = ['Tipologia', 'Data source']
ADDITION_FIELDS = ['Cantidad',
                   'Completitud',
                   'Nivel de informacion',
                   'Veracidad',
                   'Veracidad desconocida',
                   'Relevancia alta',
                   'Relevancia media',
                   'Relevancia baja',
                   'Relevancia desconocida']
CONCATENATION_FIELDS = SORT_FIELDS + ADDITION_FIELDS


# Definicion numerica de las valoraciones de las dimiensiones de calidad:
GOOD_LEVEL = 3
ACCEPTABLE_LEVEL = 2
BAD_LEVEL = 1
NO_LEVEL = 0

# Definicion numerica de las valoraciones de consistencia:
EQUIVALENCIA_CONSISTENCIA_NUMERICA = {'Low': 0,
                                      'Medium': 1,
                                      'High': 2,
                                      'Very high': 3}


# Definicion de los pesos de los distintos niveles de valoracion:
W1 = 1
W2 = 0.5
W3 = -1


# Definicion del cojunto de dimensiones que puntuan para la calidad:
DIMENSIONES = ['Cantidad nivel',
               'Duplicados nivel',
               'Completitud nivel',
               'Nivel de informacion nivel',
               'Veracidad nivel',
               'Veracidad desconocida nivel',
               'Frecuencia nivel',
               'Consistencia nivel',
               'Precio por dato nivel']


# Posibles valores para la clasificacion AHP
VALORES_AHP = ['1', '3', '5', '7', '9', '1/3', '1/5', '1/7', '1/9']


# Criterios de valoracion AHP
CRITERIOS_AHP = ['Quantity',
                 'Duplicity',
                 'Completeness',
                 'Information level',
                 'Veracity',
                 'Unknown veracity']


# Definicion de constantes para los reports:
EVENT_TYPOLOGY = 'Tipologia'
TITULO_FUENTES = 'Quality report: data source'
TITULO_TIPOLOGIAS = 'Quality report: event typology'
TITULO_RANKING = 'Data source ranking'
TITULO_PORCENTAJE_POR_EVENTO = 'Percentage of events and list of events, in each event class'
TITULO_FUENTES_POR_CLASE = 'Data source types'
TITULO_FUENTES_POR_EVENTO = 'Exclusivity results'
TITULO_CATEGORIAS_POR_EVENTO = 'Quality by event class'
TITULO_CALIDAD_EVENTOS_POR_TIPO = 'Quality by event type'
TITULO_CALIDAD_FUENTES_POR_TIPO = 'Quality by data source type'
TITULO_CALIDAD_FUENTES_POR_DIMENSION = 'Data source quality by dimension'
TITULO_EVENTOS_CUBIERTOS_POR_FUENTES = 'Events covered by data sources'
TITULO_NUMERO_DATOS_POR_FUENTE = 'Number of data records provided by data source'
DATA_SOURCE = 'Data source'
FUENTE = 'Fuente de datos'


# Definicion del conjunto de dimensiones que se mostraran graficamente:
COMPARISON_PLOTS_DIMENSIONS = ['Cantidad',
                               'Completitud',
                               'Nivel de informacion',
                               'Veracidad',
                               'Veracidad desconocida',
                               'Consistencia',
                               'Precio por dato']


# Definicion de las clases de eventos y sus codigos segun taxonomia Incibe:
DICT_CLASES_EVENTOS = {'C01': 'Abusive content',
                       'C02': 'Harmful content',
                       'C03': 'Information gathering',
                       'C04': 'Intrusion atempt',
                       'C05': 'Intrusion',
                       'C06': 'Availability',
                       'C07': 'Information commitment',
                       'C08': 'Fraud',
                       'C09': 'Vulnerabilities',
                       'C10': 'Others',
                      }


# Mensajes de pantalla:
INPUT_MSG_001 = 'Indicate the value separator character, in csv file: '
INPUT_MSG_002 = 'Indicate the period (in days) to which the sample refers: '

INPUT_MSG_003 = 'If you want to weight the APH criteria, enter Y, otherwise, enter N: '


OUTPUT_MSG_001 = "Next, you have to assess the importance of the different evaluation criteria comparing them in pairs."
OUTPUT_MSG_002 = "Select the appropriate option for each pair of criteria:"
OUTPUT_MSG_003 = "1:   Equally important"
OUTPUT_MSG_004 = "3:   Moderately more important"
OUTPUT_MSG_005 = "5:   Strongly more important"
OUTPUT_MSG_006 = "7:   Very strongly more important"
OUTPUT_MSG_007 = "9:   Extremely more important"
OUTPUT_MSG_008 = "1/3: Moderately less important"
OUTPUT_MSG_009 = "1/5: Strongly less important"
OUTPUT_MSG_010 = "1/7: Very strongly less important"
OUTPUT_MSG_011 = "1/9: Extremely less important"


INPUT_MSG_004 = 'Indicate the importance of Quantity over Duplicity: '
INPUT_MSG_005 = 'Indicate the importance of Quantity over Completeness: '
INPUT_MSG_006 = 'Indicate the importance of Quantity over Information level: '
INPUT_MSG_007 = 'Indicate the importance of Quantity over Veracity: '
INPUT_MSG_008 = 'Indicate the importance of Quantity over Unknown veracity: '
INPUT_MSG_009 = 'Indicate the importance of Duplicity over Completeness: '
INPUT_MSG_010 = 'Indicate the importance of Duplicity over Information level: '
INPUT_MSG_011 = 'Indicate the importance of Duplicity over Veracity: '
INPUT_MSG_012 = 'Indicate the importance of Duplicity over Unknown veracity: '
INPUT_MSG_013 = 'Indicate the importance of Completeness over Information level: '
INPUT_MSG_014 = 'Indicate the importance of Completeness over Veracity: '
INPUT_MSG_015 = 'Indicate the importance of Completeness over Unknown veracity: '
INPUT_MSG_016 = 'Indicate the importance of Information level over Veracity: '
INPUT_MSG_017 = 'Indicate the importance of Information level over Unknown veracity: '
INPUT_MSG_018 = 'Indicate the importance of Veracity over Unknown veracity: '


# Mensajes de aviso:
WARNING_MSG_101A = 'WARNING: Data source \"'
WARNING_MSG_101B = '\" configuration could not be loaded. Please, check file '


# Mensajes de error:
ERROR_MSG_201 = 'ERROR: Data source configuration file can not be opened'
ERROR_MSG_202 = 'ERROR: Event typology configuration file can not be opened'
ERROR_MSG_203 = 'ERROR: No se han encotrado ficheros de entrada'
ERROR_MSG_204 = 'ERROR: Data sample file can not be opened'
ERROR_MSG_205 = 'ERROR: Configuration file error: settings are not valid for data sample'
ERROR_MSG_206 = 'ERROR: Data sample file can not be opened'
ERROR_MSG_207 = 'ERROR: Configuration file error: attribute campos_obligatorios does not exist'
ERROR_MSG_208 = 'ERROR: Configuration file error: atribute %s does not exist'
ERROR_MSG_209 = 'ERROR: Merging pdfs failed. File: '
ERROR_MSG_210 = 'ERROR: Merging pdfs failed. Unable to create global report'



####################################################################################################
#                                                                                                  #
# DEFINICION DE FUNCIONES                                                                          #
#                                                                                                  #
####################################################################################################

def leer_criterios_ahp():
    """
    Reads the pairwise comparison matrix of the criteria for the analytic hierarchy process . The
    decision maker expresses how two criteria or alternatives compare to each other.

    Parameters
    ----------
    None

    Returns
    -------
    PCcriteria: numpy array
                Pairwise comparison matrix of the criteria
    """

    consistente = False
    while not consistente:
        PCcriteria = np.array([[1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                               [0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
                               [0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
                               [0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                               [0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                               [0.0, 0.0, 0.0, 0.0, 0.0, 1.0]])

        print(OUTPUT_MSG_001)
        print(OUTPUT_MSG_002)
        print(OUTPUT_MSG_003)
        print(OUTPUT_MSG_004)
        print(OUTPUT_MSG_005)
        print(OUTPUT_MSG_006)
        print(OUTPUT_MSG_007)
        print(OUTPUT_MSG_008)
        print(OUTPUT_MSG_009)
        print(OUTPUT_MSG_010)
        print(OUTPUT_MSG_011)

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_004)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[0, 1] = importancia
                PCcriteria[1, 0] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_005)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[0, 2] = importancia
                PCcriteria[2, 0] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_006)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[0, 3] = importancia
                PCcriteria[3, 0] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_007)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[0, 4] = importancia
                PCcriteria[4, 0] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_008)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[0, 5] = importancia
                PCcriteria[5, 0] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_009)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[1, 2] = importancia
                PCcriteria[2, 1] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_010)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[1, 3] = importancia
                PCcriteria[3, 1] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_011)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[1, 4] = importancia
                PCcriteria[4, 1] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_012)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[1, 5] = importancia
                PCcriteria[5, 1] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_013)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[2, 3] = importancia
                PCcriteria[3, 2] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_014)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[2, 4] = importancia
                PCcriteria[4, 2] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_015)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[2, 5] = importancia
                PCcriteria[5, 2] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_016)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[3, 4] = importancia
                PCcriteria[4, 3] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_017)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[3, 5] = importancia
                PCcriteria[5, 3] = 1/importancia
            else:
                lectura = 0

        importancia = 0
        while importancia == 0:
            lectura = input(INPUT_MSG_018)
            if lectura in VALORES_AHP:
                importancia = float(Fraction(lectura))
                PCcriteria[4, 5] = importancia
                PCcriteria[5, 4] = 1/importancia
            else:
                lectura = 0

        # the number of the criteria
        n = 6

        # random indices for consistency checking
        RI = [0, 0, 0.58, 0.90, 1.12, 1.24, 1.32, 1.41, 1.45, 1.49]

        # consistency check for pairwise comparison matrix of the criteria
        lambdamax = np.amax(np.linalg.eigvals(PCcriteria).real)
        CI = (lambdamax - n) / (n - 1)
        CR = CI / RI[n - 1]
        print("Inconsistency index of the criteria: ", CR)
        if CR > 0.1:
            print(PCcriteria)
            print("The pairwise comparison matrix of the criteria is inconsistent")
        else:
            consistente = True


    return PCcriteria



####################################################################################################
def leer_caracteristicas_muestra():
    """
    Reads the character to separate data sample files values and the period of time to which the
    data refer (in days).

    Parameters
    ----------
    None

    Input
    -----
    sep: input from keyboard
         Character to separate values in the .csv data file.
    per: input from keyboard
         Number of days to which the data refer.
    ponderar: input from keyboard
              Decision whether or not to read the  pairwise comparison matrix of the criteria

    Returns
    -------
    sep: char
         Character to separate data saple files values
    per: float
         Period of time to which the data refer (in days)
    PCcriteria: numpy array
                Pairwise comparison matrix of the criteria
    """

    sep = input(INPUT_MSG_001)
    per = float(input(INPUT_MSG_002))

    ponderar = "X"
    while ponderar not in ('Y', 'y', 'N', 'n'):
        ponderar = input(INPUT_MSG_003)

    if ponderar in ('N', 'n'):
        PCcriteria = np.array([[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                               [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                               [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                               [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                               [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                               [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]])
    else:
        PCcriteria = leer_criterios_ahp()


    return sep, per, PCcriteria



####################################################################################################

def cargar_configuracion_fuentes():
    """
    Loads the .ini datasources configuration file into a configparser.

    Parameters
    ----------
    None

    Returns
    -------
    d_s_p: ConfigParser
           Datasources configuration structure
    """

    path_to_configuration_data_source_file = os.path.join(BASE_PATH,
                                                          CONFIG_DIR,
                                                          DATA_SOURCE_CONFIG_FILE)
    d_s_p = conp.ConfigParser()
    try:
        d_s_p.read(path_to_configuration_data_source_file, encoding=ENCODING)
    except Exception:
        print(ERROR_MSG_201)
        sys.exit()


    return d_s_p



####################################################################################################
def cargar_configuracion_tipologias():
    """
    Loads the .ini event tipologies configuration file into a configparser.

    Parameters
    ----------
    None

    Returns
    -------
    e_t_p: configparser
           Event typologies configuration structure
    """

    path_to_configuration_event_typology_file = os.path.join(BASE_PATH,
                                                             CONFIG_DIR,
                                                             EVENT_TYPOLOGY_CONFIG_FILE)
    e_t_p = conp.ConfigParser()
    try:
        e_t_p.read(path_to_configuration_event_typology_file, encoding=ENCODING)
    except Exception:
        print(ERROR_MSG_202)
        sys.exit()


    return e_t_p



####################################################################################################
def cargar_ficheros_input():
    """
    Obtains the list of data sample files.

    Parameters
    ----------
    None

    Returns
    -------
    lista_fic_input: list
                     List of data sample files .csv, contained in the input directory
    """

    path_to_input_files = os.path.join(BASE_PATH, INPUT_DIR)

    lista_fic_input = glob.glob(path_to_input_files + '*.csv')

    if  not lista_fic_input:
        print(ERROR_MSG_203)
        sys.exit()


    return lista_fic_input



####################################################################################################
def borrar_salida():
    """
    Delete the output files of previous executions.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    path = os.path.join(BASE_PATH, OUTPUT_DIR)
    files = glob.glob(path + '*.*')
    for f in files:
        os.remove(f)

    path = os.path.join(BASE_PATH, DUPLICATE_DIR)
    files = glob.glob(path + '*.*')
    for f in files:
        os.remove(f)

    files = glob.glob(TEMPORAL_PATH + '*.png')
    for f in files:
        os.remove(f)


    return



####################################################################################################
def cargar_fichero_muestra_by_chunks(fic, separ):
    """
    Loads a chunk of the .csv data file into a dataframe.

    Parameters
    ----------
    fic: string
         File name
    separ: char
           Character to separate values in the .csv data file

    Returns
    -------
    dat: pandas dataframe
         Data sample
    """

    dat = pd.DataFrame()
    path_to_sample_file = os.path.join(BASE_PATH, INPUT_DIR, fic)
    try:
        dat = pd.read_csv(path_to_sample_file, sep=separ, chunksize=CHUNKSIZE)
    except Exception:
        print(ERROR_MSG_206)
        sys.exit()


    return dat



####################################################################################################
def inicializar_estructura_valoracion(t_f, d_s_p, e_t_p, no_parametrizadas):
    """
    Initializes the evaluation structure with all "datasource-event typology" combinations.
    In each item, sets datasource properties using .ini configuration file.
    If datasource is not defined in the .ini configuration file, it is not included in the return
    dataframe.
    If return dataframe is empty (there is no any datatasource defined in the .ini configuration
    file), the program returns an error and ends.

    Parameters
    ----------
    t_f: pandas dataframe
         Set of Event typology - Data source
    d_s_p: ConfigParser
           Datasource configuration structure
    e_t_p: ConfigParser
           Event typologies configuration structure
    no_parametrizadas: list
                       List of non-parameterized data sources

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    no_parametrizadas: list
                       List of non-parameterized data sources
    """

    val = pd.DataFrame(columns=('Tipologia',
                                'Clase tipologia',
                                'Data source',
                                'Data source type',
                                'Valoracion datos obsoletos',
                                'Tasa falsos positivos',
                                'Cantidad',
                                'Cantidad normalizada',
                                'Cantidad nivel',
                                'Cantidad AHP',
                                'Duplicados',
                                'Duplicados normalizada',
                                'Duplicados nivel',
                                'Duplicados AHP',
                                'Completitud',
                                'Numero campos obligatorios',
                                'Completitud normalizada',
                                'Completitud nivel',
                                'Completitud AHP',
                                'Nivel de informacion',
                                'Nivel de informacion normalizada',
                                'Nivel de informacion nivel',
                                'Nivel de informacion AHP',
                                'Veracidad',
                                'Veracidad normalizada',
                                'Veracidad nivel',
                                'Veracidad AHP',
                                'Veracidad desconocida',
                                'Veracidad desconocida normalizada',
                                'Veracidad desconocida nivel',
                                'Veracidad desconocida AHP',
                                'Frecuencia',
                                'Frecuencia normalizada',
                                'Frecuencia nivel',
                                'Consistencia',
                                'Consistencia normalizada',
                                'Consistencia nivel',
                                'Relevancia alta',
                                'Relevancia alta normalizada',
                                'Relevancia media',
                                'Relevancia media normalizada',
                                'Relevancia baja',
                                'Relevancia baja normalizada',
                                'Relevancia desconocida',
                                'Relevancia desconocida normalizada',
                                'Precio',
                                'Precio por dato',
                                'Precio por dato normalizada',
                                'Precio por dato nivel',
                                'Valoracion manual',
                                'Calidad',
                                'Exclusividad',
                                'AHP score'))

    for i in range(len(t_f)):
        tipologia = t_f.iloc[i][FIELD_TYPOLOGY]
        fuente = t_f.iloc[i][FIELD_DATA_SOURCE]
        try:
            tipo_fuente = d_s_p.get(fuente, 'type',)
        except Exception:
            tipo_fuente = "Unknown"

        try:
            val = val.append({'Tipologia': tipologia,
                              'Clase tipologia': e_t_p.get(tipologia, 'class'),
                              'Data source': fuente,
                              'Data source type': tipo_fuente,
                              'Valoracion datos obsoletos': d_s_p.get(fuente, 'obsolete_ data_evaluation',),
                              'Tasa falsos positivos': d_s_p.get(fuente, 'false_positive_rate',),
                              'Cantidad': 0,
                              'Completitud': 0,
                              'Nivel de informacion': 0,
                              'Veracidad': 0,
                              'Veracidad desconocida': 0,
                              'Frecuencia': d_s_p.get(fuente, 'frecuency'),
                              'Frecuencia normalizada': d_s_p.get(fuente, 'frecuency'),
                              'Consistencia': d_s_p.get(fuente, 'consistency'),
                              'Consistencia normalizada': d_s_p.get(fuente, 'consistency'),
                              'Relevancia alta': 0,
                              'Relevancia media': 0,
                              'Relevancia baja': 0,
                              'Relevancia desconocida': 0,
                              'Precio': float(d_s_p.get(fuente, 'price')),
                              'Valoracion manual': d_s_p.get(fuente, 'manual_evaluation'),
                              'AHP score': 'N/A'
                             }, ignore_index=True)
        except Exception:
            no_parametrizadas.append(fuente)

    if val.empty:
        print(ERROR_MSG_205)
        sys.exit()


    return val, no_parametrizadas



####################################################################################################
def eliminar_columnas_innecesarias(dat, e_t_p, l_t):
    """
    Starting from the list of typologies defined in the configuration file and included in the data
    file, this function deletes from the data sample all those features that are not necessary for
    the evaluation of the quality of the data.

    Parameters
    ----------
    dat: pandas dataframe
         Data sample
    e_t_p: ConfigParser
           Event typology configuration structure
    l_t: list
         List of event tipologies

    Returns
    -------
    data_reducido: pandas dataframe
                   Reduced data sample (without unnecessary features)

    Returns
    -------
    None
    """

    campos_necesarios = [FIELD_TYPOLOGY,
                         FIELD_DATA_SOURCE,
                         FIELD_FIABILITY,
                         FIELD_SEVERITY, FIELD_ID]

    tmp = e_t_p.get('Default Section', 'mandatory_fields')
    tmp = tmp.replace('\n', '')
    campos_config = tmp.split(",")

    campos_necesarios += campos_config

    for tip in l_t:
        try:
            tmp = e_t_p.get(tip, 'mandatory_fields')
            tmp = tmp.replace('\n', '')
            campos_config = tmp.split(",")

            campos_necesarios += campos_config

        except Exception:
            pass

        try:
            tmp = e_t_p.get(tip, 'key_fields')
            tmp = tmp.replace('\n', '')
            campos_config = tmp.split(",")

            campos_necesarios += campos_config

        except Exception:
            pass

    campos_necesarios = set(campos_necesarios)

    data_reducido = dat.loc[:, campos_necesarios]


    return data_reducido



####################################################################################################
def replace_by_threshold(df_data, column, threshold, recodified):
    """
    Replace the column by the values in recodified as specified by threshold

    Parameters
    ----------
    df_data: pandas DataFrame
             Data sample
    column: object
            The column whose values will be replaced
    threshold: list
               The threshold values. Its lenght must be equal to 2
    recodified: list
                The values to be substituted into the data column. Its lenght must be equal to 3
    """

    col_data = pd.to_numeric(df_data[column], errors='coerce')
    nulled = pd.isnull(col_data)
    lower = (col_data <= threshold[0]) & (col_data > 1)
    middle = (col_data > threshold[0]) & (col_data < threshold[1])
    higher = col_data >= threshold[1]
    col_data[higher] = recodified[-1]
    col_data[middle] = recodified[-2]
    col_data[lower] = recodified[-3]
    col_data[nulled] = 1
    df_data[column] = col_data



####################################################################################################
def redefinir_datos_fiabilidad_severidad(dat):
    """
    Modifies the data sample to standardize severity and fiability values.

    Parameters
    ----------
    dat: pandas dataframe
         Data sample

    Returns
    -------
    dat: pandas dataframe
         Data sample with standardized reliability and severity values
    """

    threshold_split = [4, 8]
    valores_recodificados = [3, 6, 9]

    replace_by_threshold(dat, FIELD_SEVERITY, threshold_split, valores_recodificados)
    replace_by_threshold(dat, FIELD_FIABILITY, threshold_split, valores_recodificados)


    return dat



####################################################################################################
def obtener_campos_obligatorios(tip, e_t_p):
    """
    Return the mandatory fields for the selected typology. This fields are collected from the
    configuration file.
    First, the field names will be tried to recover from the typology section.
    If they are not defined, they are retrieved from default section.
    This list will be used as a reference to consider whether a data is complete or not.

    Parameters
    ----------
    tip: string
         Event typology
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    cam_obl: string list
             Mandatory fields for tip typology, sepparated by commas
    """

    cam_obl = []
    try:
        tmp = e_t_p.get(tip, 'mandatory_fields')
        tmp = tmp.replace('\n', '')
        cam_obl = tmp.split(",")
    except Exception:
        try:
            tmp = e_t_p.get('Default Section', 'mandatory_fields')
            tmp = tmp.replace('\n', '')
            cam_obl = tmp.split(",")
        except Exception:
            print(ERROR_MSG_207)
            sys.exit()


    return cam_obl



####################################################################################################
def valorar_completitud(val, dat, i, tip, e_t_p):
    """
    Evaluates data completeness. This function checks how many data contain all mandatory fields,
    for a specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    dat: pandas dataframe
         Data sample for the current Data source and Event typology
    i: integer
       Current index in dataframe val
    tip: string
         Event typology
    e_t_p: ConfigParser
           Event typology configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    # Calculo de la completitud para la tipologia-fuente actual
    campos_obligatorios = obtener_campos_obligatorios(tip, e_t_p)

    #data_compl = dat.dropna(subset=campos_obligatorios)

    #completitud = len(data_compl)
    mask = pd.notnull(dat[campos_obligatorios]).values
    completitud = np.sum(np.all(mask, axis=1))

    # Almacenamos el numero de campos obligatorios para despues normalizar
    val.loc[i, 'Numero campos obligatorios'] = len(campos_obligatorios)

    campos_totales = np.sum(mask)
    modified = ['Completitud', 'Nivel de informacion']
    #val.loc[i, 'Nivel de informacion'] += campos_totales
    val.loc[i, modified] += [completitud, campos_totales]


    return val



####################################################################################################
def obtener_parametro(parametro, tip, c_p):
    """
    Get an item value from comfiguration file. First, it is looked for in the typology section. If
    it does not exist, it is looked for in the defalut section.

    Parameters
    ----------
    parametro: string
               .ini file key which we want to obtain the value.
    tip: string
         Event typology
    c_p : ConfigParser
          Configuration file values

    Returns
    -------
    valor_parametro: string
                     Value of the configuration file element with key = parameter
    """

    try:
        valor_parametro = c_p.get(tip, parametro)
    except Exception:
        try:
            valor_parametro = c_p.get('Default Section', parametro)
        except Exception:
            print(ERROR_MSG_208, parametro)
            sys.exit()


    return valor_parametro



####################################################################################################
def valorar_veracidad(val, dat, i, tip, e_t_p):
    """
    Evaluates data accuracy/credibility (reliability).
    This function checks:
        · Reliability: How many data reach the reference reliability level, for a specific event
          typology.
        . Unknow reliability level: What is the unknow reliability level in received data, for a
          specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    dat: pandas dataframe
         Data sample for the current Data source and Event typology
    i: integer
       Current index in dataframe val
    tip: string
         Event typology
    e_t_p: ConfigParser
           Event typology configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    # Calculo de la veracidad para la tipologia-fuente actual
    veracidad_referencia = int(obtener_parametro('veracity_reference', tip, e_t_p))
    veracidad = np.sum(dat[FIELD_FIABILITY] >= veracidad_referencia)

    # Calculo de la veracidad desconocida para la tipologia-fuente actual
    #veracidad_desconocida = len(dat[dat[FIELD_FIABILITY] <= 1])
    veracidad_desconocida = np.sum(dat[FIELD_FIABILITY] <= 1)
    #val.loc[i, 'Veracidad desconocida'] += veracidad_desconocida

    val.loc[i, ['Veracidad', 'Veracidad desconocida']] += [veracidad, veracidad_desconocida]


    return val



####################################################################################################
def valorar_relevancia(val, dat, i):
    """
    Evaluates the data distribution into the different levels of severity.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    dat: pandas dataframe
         Data sample for the current Data source and Event typology
    i: integer
       Current index in dataframe val

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    # Calculo de la relevancia alta para la tipologia-fuente actual
    #relevancia_alta = len(dat[dat[FIELD_SEVERITY] >= 8])
    relevancia_alta = np.sum(dat[FIELD_SEVERITY] >= 8)
    #val.loc[i, 'Relevancia alta'] += relevancia_alta

    # Calculo de la relevancia media para la tipologia-fuente actual
    #relevancia_media = len(dat(dat[FIELD_SEVERITY] >= 5) & (dat[FIELD_SEVERITY] < 8)])
    relevancia_media = np.sum((dat[FIELD_SEVERITY] >= 5) & (dat[FIELD_SEVERITY] < 8))
    #val.loc[i, 'Relevancia media'] += relevancia_media

    # Calculo de la relevancia baja para la tipologia-fuente actual
    #relevancia_baja = len(dat(dat[FIELD_SEVERITY] >= 2) & (dat[FIELD_SEVERITY] < 5)])
    relevancia_baja = np.sum((dat[FIELD_SEVERITY] >= 2) & (dat[FIELD_SEVERITY] < 5))
    #val.loc[i, 'Relevancia baja'] += relevancia_baja

    # Calculo de la relevancia desconocida para la tipologia-fuente actual
    #relevancia_desconocida = len(dat(dat[FIELD_SEVERITY] < 2)])
    relevancia_desconocida = np.sum(dat[FIELD_SEVERITY] < 2)
    #val.loc[i, 'Relevancia desconocida'] += relevancia_desconocida

    relevancias = ['Relevancia alta',
                   'Relevancia media',
                   'Relevancia baja',
                   'Relevancia desconocida']
    val.loc[i, relevancias] += [relevancia_alta,
                                relevancia_media,
                                relevancia_baja,
                                relevancia_desconocida]


    return val



####################################################################################################
def eliminar_tildes(val):
    """
    Remove tildes from evaluation dataframe's typology variable.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    val['Tipologia'] = val['Tipologia'].apply(lambda x: unicodedata.normalize('NFKD', x.decode('utf-8')).encode('utf-8').decode('ascii', 'ignore'))


    return val



####################################################################################################
def process_chunk(data, d_s_p, e_t_p, no_parametrizadas):
    """
    Process a data chunk from a data file and creates the evaluation structure for the data
    contained within.

    Parameters
    ----------
    data: pandas.Dataframe
          A chunk from a data file
    d_s_p: ConfigParser
           Datasources configuration structure
    e_t_p: ConfigParser
           Event typologies configuration structure
    no_parametrizadas: list
                       List of non-parameterized data sources

    Returns
    -------
    valoracion: pandas.DataFrame
                Evaluation structure for each Data source - Event typology in the chunk
    no_parametrizadas: list
                       List of non-parameterized data sources
    """

    #try:
    # data.to_csv('chunks/%f.csv' % np.random.random(), header=True, index=False)
    pairs = list(set(zip(data[FIELD_TYPOLOGY], data[FIELD_DATA_SOURCE])))
    tip_fue = pd.DataFrame(data=pairs, columns=[FIELD_TYPOLOGY, FIELD_DATA_SOURCE])
    valoracion, no_parametrizadas = inicializar_estructura_valoracion(tip_fue, d_s_p, e_t_p, no_parametrizadas)
    lista_tipologias = list(set(valoracion['Tipologia']))
    data = eliminar_columnas_innecesarias(data, e_t_p, lista_tipologias)
    data = redefinir_datos_fiabilidad_severidad(data)

    # itera solo sobre los pares tipologia - fuente que sabemos que estan presentes
    for tipologia, fuente in zip(valoracion['Tipologia'], valoracion['Data source']):
        data_aux = data[(data[FIELD_TYPOLOGY] == tipologia) & (data[FIELD_DATA_SOURCE] == fuente)]

        # Calculo de medidas relacionadas con la dimension de CANTIDAD (I):
        #       La cantidad normalizada y el nivel de calidad se calcularan al
        #       final del proceso, ya que necesitan utilizar los datos de todas
        #       las fuentes
        cantidad = int(len(data_aux))
        # Indice del par tipologia-fuente en la estructura de valoracion
        i = np.nonzero((((valoracion['Tipologia'] == tipologia) & (valoracion['Data source'] == fuente))).to_numpy())[0][0]
        valoracion.loc[i, 'Cantidad'] += cantidad

        # Calculo de medidas relacionadas con la dimension de COMPLETITUD:
        valoracion = valorar_completitud(valoracion, data_aux, i, tipologia, e_t_p)

        # Calculo de medidas relacionadas con la dimension de VERACIDAD:
        valoracion = valorar_veracidad(valoracion, data_aux, i, tipologia, e_t_p)

        # Calculo de medidas relacionadas con la dimension de RELEVANCIA:
        valoracion = valorar_relevancia(valoracion, data_aux, i)


    # Genracion de csv para la busqueda de duplicados
    for tipologia in set(valoracion['Tipologia']):
        campos_clave = [FIELD_DATA_SOURCE, FIELD_ID]
        try:
            tmp = e_t_p.get(tipologia, 'key_fields')
            tmp = tmp.replace('\n', '')
            cam_cl = tmp.split(",")
        except Exception:
            continue

        campos_clave += cam_cl
        campos_clave = set(campos_clave)

        fic = tipologia + ".csv"

        path_to_duplicate_file = os.path.join(BASE_PATH, DUPLICATE_DIR, fic)

        data_aux = data[(data[FIELD_TYPOLOGY] == tipologia)]
        data_aux = data_aux[campos_clave]

        data_aux.to_csv(path_to_duplicate_file,
                        sep=',',
                        index=None,
                        quoting=csv.QUOTE_NONNUMERIC,
                        mode="a",
                        header=not os.path.isfile(path_to_duplicate_file)
                        )


    return valoracion, no_parametrizadas

# Posible problema: tener una lista de 10 ** 6 o mas estructuras de valoracion
# parciales si el numero de chunks totales es muy elevado. En este caso
# seria mejor que process_chunk no devolviese la estrucutra de valoracion
# sino que la guardase con to_csv(mode='a'). Luego se podria leer el archivo
# resultante por chunks y ejecutar compute_valoracion progresivamente.



####################################################################################################
def valorar_dimensiones(lis_fic, separ, d_s_p, e_t_p):
    """
    Create evaluation structures for all input files.

    Parameters
    ----------
    lis_fic: list.
             List of data sample files .csv, contained in the input directory
    separ: char
           Character to separate values in the .csv data file
    d_s_p: ConfigParser
           Datasource configuration structure
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    val = pd.DataFrame()
    i = 0
    no_parametrizadas = []
    for path in lis_fic:
        print("Processing file ", path)
        reader = cargar_fichero_muestra_by_chunks(path, separ)
        for chunk in reader:
            val_aux = pd.DataFrame()
            val_aux, no_parametrizadas = process_chunk(chunk, d_s_p, e_t_p, no_parametrizadas)
            val = val.append(val_aux)
            del chunk
            del val_aux
            i += 1
            gc.collect()
        reader.close()
        del reader

    no_parametrizadas = list(set(no_parametrizadas))
    no_parametrizadas.sort()
    for fuente in no_parametrizadas:
        print(WARNING_MSG_101A, fuente, WARNING_MSG_101B, DATA_SOURCE_CONFIG_FILE, sep='')


    return val



####################################################################################################
def compute_valoracion(valoracion_chunks):
    """
    Combine all of the partial evaluation strucutres computed per chunk.

    Parameters
    ----------
    valoracion_chunks: list<pandas.DataFrame>
                       A list of all the evaluation structures computed per chunk

    Returns
    -------
    valoracion: pandas.DataFrame
                Evaluation structure for each Data source - Event typology
    """

#    res = pd.concat(valoracion_chunks)
#    del valoracion_chunks

    # Selecciona unicamente los valores para los cuales tiene sentido sumar
    grouped = valoracion_chunks[CONCATENATION_FIELDS]

    # Agrupa por tipologia y fuente y suma
    # grouped = grouped.groupby(by=SORT_FIELDS, as_index=False).sum(axis=0)
    grouped = grouped.groupby(by=SORT_FIELDS, as_index=False).sum()

    # Genera una estructura de valoracion con una sola fila para cada par tipologia-fuente y ordena
    # las filas por tipologia y fuente
    # Este ordenamiento se hace para despues poder insertar las columnas con las dimensiones sumadas
    # anteriores
    valoracion = valoracion_chunks.drop_duplicates(subset=SORT_FIELDS).sort_values(by=SORT_FIELDS)

    # Ordena la suma por tipologia-fuente. Asi las filas de valoracion y grouped estaran alineadas
    grouped = grouped.sort_values(by=SORT_FIELDS)

    # Se reinicia el indice para que consista en enteros ascendentes y contiguos. Si no se hace
    # esto, la siguiente sentencia fracasara horriblemente
    valoracion.reset_index(drop=True, inplace=True)

    # Sustituye los valores sumados en la estrucutra de valoracion
    valoracion[ADDITION_FIELDS] = grouped[ADDITION_FIELDS]


    return valoracion



####################################################################################################
def valorar_nivel_informacion(val):
    """
    Evaluates data information level. This function checks what is the mandatory fields average in
    received data, for a specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    val['Nivel de informacion'] = round((val['Nivel de informacion'] / val['Cantidad']), 3)


    return val



####################################################################################################
def valorar_precio_por_dato(val, d_p):
    """
    Evaluates economic value of the data.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    d_p: int
         Period of time to which the data refer (in days)

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    # Diccionario para almacenar las cantidades de datos totales aportadas por cada tipologia
    dict_cantidad_por_fuente = {}
    for fuente in set(val['Data source']):
        df_aux = val[val['Data source'] == fuente]
        total = sum(df_aux['Cantidad'])
        dict_aux = {fuente: total}
        dict_cantidad_por_fuente.update(dict_aux)

    del df_aux

    # Se recorre el datasource de valoracion y se va completando el dato de precio por dato
    for i in range(len(val)):
        fue = val.loc[i, 'Data source']

        # Calculo del precio por dato para la tipologia-fuente actual
        precio = val.loc[i, 'Precio']
        cantidad_fuente = dict_cantidad_por_fuente.get(fue)
        precio_por_dato = (float(precio) * float(d_p)) / (float(cantidad_fuente) * 365)
        precio_por_dato = round(precio_por_dato, 6)


        val.loc[i, 'Precio por dato'] = precio_por_dato


    return val



####################################################################################################
def encontrar_duplicados(val, e_t_p):
    """
    Evaluate duplication dimension.
    Read key fields csv files for each event typology, and determine the number of duplicates.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    valoracion: pandas dataframe
                Evaluation structure for each Data source - Event typology
    """

    valoracion = val

    # Inicializar valoracion de duplicados (si no hay campos clave definidos se
    #   inicializa con N/A, en caso contrario con 0)
    for i in range(len(valoracion)):
        tip = valoracion.loc[i, 'Tipologia']

        try:
            tmp = e_t_p.get(tip, 'key_fields')
            valoracion.loc[i, 'Duplicados'] = 0
        except Exception:
            valoracion.loc[i, 'Duplicados'] = "N/A"


    for tipologia in set(val['Tipologia']):
        fic = tipologia + ".csv"
        path_to_duplicate_file = os.path.join(BASE_PATH, DUPLICATE_DIR, fic)

        try:
            data_aux = pd.read_csv(path_to_duplicate_file, sep=',')
        except Exception:
            continue

        try:
            tmp = e_t_p.get(tipologia, 'key_fields')
            tmp = tmp.replace('\n', '')
            campos_clave = tmp.split(",")
            campos_clave = set(campos_clave)
            campos_clave = list(campos_clave)

        except Exception:
            continue

        df_duplicados = pd.DataFrame()
        indices = data_aux.drop_duplicates(subset=campos_clave, keep=False).index
        df_duplicados = data_aux.drop(indices, axis=0)

        df_duplicados.sort_values(campos_clave, inplace=True)

        df_duplicados.to_csv(path_to_duplicate_file,
                             sep=',',
                             index=None,
                             mode="w",
                             header=True
                             )

        # Para cada valoracion de la tipologia, contar numero filtrando por fuente
        for fuente in set(df_duplicados[FIELD_DATA_SOURCE]):
            data_tmp = df_duplicados[(df_duplicados[FIELD_DATA_SOURCE] == fuente)]
            total_duplicados = data_tmp.shape[0]

            indice = valoracion[(valoracion['Data source'] == fuente) & (valoracion['Tipologia'] == tipologia)].index.tolist()
            if len(indice) == 1:
                indice = indice[0]
            elif len(indice) > 1:
                print('ERROR')

            valoracion.loc[indice, 'Duplicados'] = total_duplicados


    return valoracion



####################################################################################################
def calcular_cantidad_normalizada(val):
    """
    It calculates the normalized value of Quantity dimension (Quantity divided by largest Quantity
    for that typology provided by any data source).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    # Diccionario para almacenar los datos maximos de una tipologia, aportados por una sola fuente
    dict_tipologias_maximo = {}
    for tipologia in set(val['Tipologia']):
        df_aux = val[val['Tipologia'] == tipologia]
        maximo = max(df_aux['Cantidad'])
        dict_aux = {tipologia: maximo}
        dict_tipologias_maximo.update(dict_aux)

    del df_aux

    # Se recorre el datasource de valoracion y se va completando el campo de cantidad normalizada
    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']

        # Calculo de la cantidad normalizada para la tipologia-fuente actual
        cantidad = val.loc[i, 'Cantidad']
        maximo_tipologia = dict_tipologias_maximo.get(tip)
        cantidad_normalizada = float(cantidad) / float(maximo_tipologia)
        cantidad_normalizada = round(cantidad_normalizada, 3)

        val.loc[i, 'Cantidad normalizada'] = cantidad_normalizada


    return val



####################################################################################################
def calcular_valores_normalizados(val):
    """
    It calculates the normalized values of completeness, information level, accuracy (reliability
    and unknow reliability) and relevance dimensions (Quality dimension divided by Quantity).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    for i in range(len(val)):
        cantidad = val.loc[i, 'Cantidad']

        # Duplicados normalizada
        if val.loc[i, 'Duplicados'] == "N/A":
            val.loc[i, 'Duplicados normalizada'] = "N/A"
        else:
            duplicados_normalizada = float(val.loc[i, 'Duplicados']) / float(cantidad)
            duplicados_normalizada = round(duplicados_normalizada, 3)
            val.loc[i, 'Duplicados normalizada'] = duplicados_normalizada

        # Completitud normalizada
        completitud_normalizada = float(val.loc[i, 'Completitud']) / float(cantidad)
        completitud_normalizada = round(completitud_normalizada, 3)
        val.loc[i, 'Completitud normalizada'] = completitud_normalizada

        # Nivel de informacion normalizada
        numero_campos_obligatorios = val.loc[i, 'Numero campos obligatorios']
        nivel_informacion_normalizada = float(val.loc[i, 'Nivel de informacion']) / float(numero_campos_obligatorios)
        nivel_informacion_normalizada = round(nivel_informacion_normalizada, 3)
        val.loc[i, 'Nivel de informacion normalizada'] = nivel_informacion_normalizada

        # Veracidad normalizada
        veracidad_normalizada = float(val.loc[i, 'Veracidad']) / float(cantidad)
        veracidad_normalizada = round(veracidad_normalizada, 3)
        val.loc[i, 'Veracidad normalizada'] = veracidad_normalizada

        # Veracidad desconocida normalizada
        veracidad_desconocida_normalizada = float(val.loc[i, 'Veracidad desconocida']) / float(cantidad)
        veracidad_desconocida_normalizada = round(veracidad_desconocida_normalizada, 3)
        val.loc[i, 'Veracidad desconocida normalizada'] = veracidad_desconocida_normalizada

        # Relevancia alta normalizada
        relevancia_alta_normalizada = float(val.loc[i, 'Relevancia alta']) / float(cantidad)
        relevancia_alta_normalizada = round(relevancia_alta_normalizada, 3)
        val.loc[i, 'Relevancia alta normalizada'] = relevancia_alta_normalizada

        # Relevancia media normalizada
        relevancia_media_normalizada = float(val.loc[i, 'Relevancia media']) / float(cantidad)
        relevancia_media_normalizada = round(relevancia_media_normalizada, 3)
        val.loc[i, 'Relevancia media normalizada'] = relevancia_media_normalizada

        # Relevancia baja normalizada
        relevancia_baja_normalizada = float(val.loc[i, 'Relevancia baja']) / float(cantidad)
        relevancia_baja_normalizada = round(relevancia_baja_normalizada, 3)
        val.loc[i, 'Relevancia baja normalizada'] = relevancia_baja_normalizada

        # Relevancia desconocida normalizada
        relevancia_desconocida_normalizada = float(val.loc[i, 'Relevancia desconocida']) / float(cantidad)
        relevancia_desconocida_normalizada = round(relevancia_desconocida_normalizada, 3)
        val.loc[i, 'Relevancia desconocida normalizada'] = relevancia_desconocida_normalizada

    # Una vez hemos calculado el nivel de informacion normalizado, borramos la columna 'Numero
    #   campos obligatorios' del dataframe, ya que no se va a utilizar mas.
    val.drop(['Numero campos obligatorios'], axis='columns', inplace=True)


    return val



####################################################################################################
def calcular_precio_normalizado(val, e_t_p):
    """
    Evaluates economic value of the data.
    It calculates the normalized economic value of the data (Price per data Reference price for this
    event typology).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    for i in range(len(val)):
        tipologia = val.loc[i, 'Tipologia']
        precio_por_dato = float(val.loc[i, 'Precio por dato'])
        precio_referencia = obtener_parametro('price_per_data_reference', tipologia, e_t_p)
        precio_por_dato_normalizada = float(precio_por_dato) / float(precio_referencia)
        precio_por_dato_normalizada = round(precio_por_dato_normalizada, 6)

        val.loc[i, 'Precio por dato normalizada'] = precio_por_dato_normalizada


    return val



####################################################################################################
def obtener_segundos(cadena):
    """
    Get the value in seconds from a string with format hh:mm:ss.

    Parameters
    ----------
    cadena: string
            Time value with hh:mm:ss format

    Returns
    -------
    segs: int
          Time value in seconds
    """

    tupla = cadena.split(":")
    segs = int(tupla[0]) * 3600 + int(tupla[1]) * 60 + int(tupla[0])


    return segs



####################################################################################################
def calcular_niveles(val, e_t_p):
    """
    It sets quality level (good, acceptable or bad) in each quality dimension.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']

        # Asignacion del nivel de cantidad
        cantidad_normalizada = val.loc[i, 'Cantidad normalizada']
        umbral_deseado = float(obtener_parametro('quantity_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('quantity_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if cantidad_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (cantidad_normalizada >= umbral_minimo) & (cantidad_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if cantidad_normalizada >= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Cantidad nivel'] = nivel
        val.loc[i, 'Cantidad AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de duplicados
        duplicados_normalizada = val.loc[i, 'Duplicados normalizada']
        umbral_deseado = float(obtener_parametro('duplicate_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('duplicate_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if duplicados_normalizada == "N/A":
            nivel = NO_LEVEL
            nivel_ahp = '0'
        elif duplicados_normalizada <= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (duplicados_normalizada <= umbral_minimo) & (duplicados_normalizada > umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if duplicados_normalizada <= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Duplicados nivel'] = nivel
        val.loc[i, 'Duplicados AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de completitud
        completitud_normalizada = val.loc[i, 'Completitud normalizada']
        umbral_deseado = float(obtener_parametro('completeness_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('completeness_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if completitud_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (completitud_normalizada >= umbral_minimo) & (completitud_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if completitud_normalizada >= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Completitud nivel'] = nivel
        val.loc[i, 'Completitud AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de nivel de informacion
        nivel_de_informacion_normalizada = val.loc[i, 'Nivel de informacion normalizada']
        umbral_deseado = float(obtener_parametro('information_level_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('information_level_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if nivel_de_informacion_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (nivel_de_informacion_normalizada >= umbral_minimo) & (nivel_de_informacion_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if nivel_de_informacion_normalizada >= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Nivel de informacion nivel'] = nivel
        val.loc[i, 'Nivel de informacion AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de veracidad
        veracidad_normalizada = val.loc[i, 'Veracidad normalizada']
        umbral_deseado = float(obtener_parametro('veracity_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('veracity_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if veracidad_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (veracidad_normalizada >= umbral_minimo) & (veracidad_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if veracidad_normalizada >= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Veracidad nivel'] = nivel
        val.loc[i, 'Veracidad AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de veracidad desonocida
        veracidad_desconocida_normalizada = val.loc[i, 'Veracidad desconocida normalizada']
        umbral_deseado = float(obtener_parametro('unknown_veracity_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('unknown_veracity_minimum', tip, e_t_p))
        umbral_medio = (umbral_deseado + umbral_minimo) / 2

        if veracidad_desconocida_normalizada <= umbral_deseado:
            nivel = GOOD_LEVEL
            nivel_ahp = 'V'
        elif (veracidad_desconocida_normalizada <= umbral_minimo) & (veracidad_desconocida_normalizada > umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
            if veracidad_desconocida_normalizada <= umbral_medio:
                nivel_ahp = 'A'
            else:
                nivel_ahp = 'N'
        else:
            nivel = BAD_LEVEL
            nivel_ahp = 'R'

        val.loc[i, 'Veracidad desconocida nivel'] = nivel
        val.loc[i, 'Veracidad desconocida AHP'] = nivel_ahp

        del umbral_deseado, umbral_minimo, nivel, nivel_ahp

        # Asignacion del nivel de frecuencia
        frecuencia_normalizada = val.loc[i, 'Frecuencia normalizada']
        frecuencia_umbral_deseado = obtener_parametro('frecuency_desired', tip, e_t_p)
        frecuencia_umbral_minimo = obtener_parametro('frecuency_minimum', tip, e_t_p)

        # Conversion de los datos a segundos, para poder compararlos
        frecuencia_normalizada_seg = obtener_segundos(frecuencia_normalizada)
        frecuencia_umbral_deseado_seg = obtener_segundos(frecuencia_umbral_deseado)
        frecuencia_umbral_minimo_seg = obtener_segundos(frecuencia_umbral_minimo)

        if frecuencia_normalizada_seg <= frecuencia_umbral_deseado_seg:
            nivel = GOOD_LEVEL
        elif (frecuencia_normalizada_seg <= frecuencia_umbral_minimo_seg) & (frecuencia_normalizada_seg > frecuencia_umbral_deseado_seg):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Frecuencia nivel'] = nivel

        del nivel

        #Asignacion del nivel de consistencia
        consistencia_normalizada = val.loc[i, 'Consistencia normalizada']
        consistencia_umbral_deseado = obtener_parametro('consistency_desired', tip, e_t_p)
        consistencia_umbral_minimo = obtener_parametro('consistency_minimum', tip, e_t_p)

        # Conversion de los datos a numericos, para poder compararlos
        consistencia_normalizada_numerica = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_normalizada)
        consistencia_umbral_deseado_numerico = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_umbral_deseado)
        consistencia_umbral_minimo_numerico = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_umbral_minimo)

        if consistencia_normalizada_numerica >= consistencia_umbral_deseado_numerico:
            nivel = GOOD_LEVEL
        elif (consistencia_normalizada_numerica >= consistencia_umbral_minimo_numerico) & (consistencia_normalizada_numerica < consistencia_umbral_deseado_numerico):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Consistencia nivel'] = nivel

        del nivel

        #Asignacion del nivel de precio por dato
        precio_por_dato_normalizada = val.loc[i, 'Precio por dato normalizada']
        umbral_deseado = float(obtener_parametro('price_per_data_desired', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('price_per_data_minimum', tip, e_t_p))

        if precio_por_dato_normalizada <= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (precio_por_dato_normalizada <= umbral_minimo) & (precio_por_dato_normalizada > umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Precio por dato nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel


    return val



####################################################################################################
def valorar_calidad_tipologia(val):
    """
    Evaluates datasource quality level in each typology.
    The quality is calculated using the levels in each data quality dimension, and applying the
    evaluation weights.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    for i in range(len(val)):
        calidad = 0.0
        for dim in DIMENSIONES:
            puntuacion = val.loc[i, dim]
            if puntuacion == GOOD_LEVEL:
                calidad = calidad + W1
            elif puntuacion == ACCEPTABLE_LEVEL:
                calidad = calidad + W2
            elif puntuacion == BAD_LEVEL:
                calidad = calidad + W3
            else:
                pass

        calidad = calidad / len(DIMENSIONES)
        calidad = round(calidad, 3)

        val.loc[i, 'Calidad'] = calidad


    return val



####################################################################################################
def valorar_exclusividad(val):
    """
    Gets the list of sources that provide data for each event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']
        val_aux = val[val['Tipologia'] == tip]
        val_aux = val_aux.reset_index()
        lista_fuentes = []
        for j in range(len(val_aux)):
            if (val_aux.loc[j, 'Data source']) != (val.loc[i, 'Data source']):
                lista_fuentes.append(val_aux.loc[j, 'Data source'])

        lista_fuentes_cadena = ', '.join(lista_fuentes)
        val.loc[i, 'Exclusividad'] = lista_fuentes_cadena


    return val



####################################################################################################
def valorar_calidad_global(val):
    """
    Evaluates datasource global quality level.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_fuentes: pandas dataframe
                 Data source evaluation structure
    """

    val_fuentes = pd.DataFrame(columns=(FIELD_DATA_SOURCE,
                                        'Tipo',
                                        'Tipologias',
                                        'Valoracion datos obsoletos',
                                        'Tasa falsos positivos',
                                        'Precio',
                                        'Valoracion manual',
                                        'Calidad',
                                        'Diversidad',
                                        'Total'))

    total_tip = len(set(val['Tipologia']))

    for fuente in set(val['Data source']):
        df_aux = val[val['Data source'] == fuente]
        t_f = df_aux.loc[df_aux.index[0], 'Data source type']
        tip = len(df_aux)
        vdo = df_aux.loc[df_aux.index[0], 'Valoracion datos obsoletos']
        tfp = df_aux.loc[df_aux.index[0], 'Tasa falsos positivos']
        pre = df_aux.loc[df_aux.index[0], 'Precio']
        pre = round(pre, 2)
        v_m = df_aux.loc[df_aux.index[0], 'Valoracion manual']
        cal = df_aux['Calidad'].mean()
        cal = round(cal, 3)
        div = float(tip) / float(total_tip)
        div = round(div, 3)
        tot = cal + div
        tot = round(tot, 3)

        val_fuentes = val_fuentes.append({FIELD_DATA_SOURCE: fuente,
                                          'Tipo': t_f,
                                          'Tipologias': tip,
                                          'Precio': pre,
                                          'Valoracion datos obsoletos': vdo,
                                          'Tasa falsos positivos': tfp,
                                          'Valoracion manual': v_m,
                                          'Calidad': cal,
                                          'Diversidad': div,
                                          'Total': tot
                                         }, ignore_index=True)

    del df_aux


    return val_fuentes


####################################################################################################
def valorar_porcentaje_por_evento(val):
    """
    Gets data for the "event percentage in each event class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_porcentaje_por_evento : pandas dataframe
                                Dataframe with percentage of data by event class and list of events
                                by event class
    """

    val_porcentaje_por_evento = pd.DataFrame(columns=('Code',
                                                      'Class of event',
                                                      'Events on data sample(%)',
                                                      'List of events'))

    total_eventos = val['Cantidad'].sum()

    for clase in set(val['Clase tipologia']):
        df_aux = val[val['Clase tipologia'] == clase]

        nombre_clase = DICT_CLASES_EVENTOS[clase]
        porcentaje_eventos = ((df_aux['Cantidad'].sum() / total_eventos) * 100).round(2)
        lista_eventos = list(set(df_aux['Tipologia']))
        lista_eventos.sort()

        val_porcentaje_por_evento = val_porcentaje_por_evento.append({'Code': clase,
                                                                      'Class of event': nombre_clase,
                                                                      'Events on data sample(%)': porcentaje_eventos,
                                                                      'List of events': lista_eventos
                                                                      }, ignore_index=True)


    return val_porcentaje_por_evento



####################################################################################################
def valorar_fuentes_por_clase(val):
    """
    Gets data for the "data source list in each data source class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_fuentes_por_clase : pandas dataframe
                            Dataframe with data source list by class
    """

    val_fuentes_por_clase = pd.DataFrame(columns=('Type',
                                                  'Data sources'))

    for clase in set(val['Data source type']):
        df_aux = val[val['Data source type'] == clase]

        lista_fuentes = list(set(df_aux['Data source']))
        lista_fuentes.sort()

        val_fuentes_por_clase = val_fuentes_por_clase.append({'Type': clase,
                                                              'Data sources': lista_fuentes
                                                              }, ignore_index=True)


    return val_fuentes_por_clase



####################################################################################################
def valorar_fuentes_por_evento(val):
    """
    Gets data for the "number of data sources in each event class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_fuentes_por_evento: pandas dataframe
                            Dataframe with number of data sources by event class
    """

    val_fuentes_por_evento = pd.DataFrame(columns=('Event class',
                                                   'Number of data sources'))

    for clase in set(val['Clase tipologia']):
        df_aux = val[val['Clase tipologia'] == clase]

        total = df_aux.shape[0]

        val_fuentes_por_evento = val_fuentes_por_evento.append({'Event class': clase,
                                                                'Number of data sources': total,
                                                                }, ignore_index=True)

        val_fuentes_por_evento.sort_values(['Event class'],
                                           ascending=[True],
                                           inplace=True)


    return val_fuentes_por_evento



####################################################################################################
def valorar_calidad_por_categorias_eventos(val):
    """
    Gets data for the "quality by event class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_categorias_evento: pandas dataframe
                           Data source evaluation structure
    """

    val_categorias_evento = pd.DataFrame(columns=('Event class',
                                                  'Quantity high',
                                                  'Quantity acceptable',
                                                  'Quantity low',
                                                  'Duplicates high',
                                                  'Duplicates acceptable',
                                                  'Duplicates low',
                                                  'Duplicates unknown',
                                                  'Completeness high',
                                                  'Completeness acceptable',
                                                  'Completeness low',
                                                  'Information level high',
                                                  'Information level acceptable',
                                                  'Information level low',
                                                  'Veracity high',
                                                  'Veracity acceptable',
                                                  'Veracity low',
                                                  'Unknown veracity high',
                                                  'Unknown veracity acceptable',
                                                  'Unknown veracity low',
                                                  'Relevance high',
                                                  'Relevance medium',
                                                  'Relevance low',
                                                  'Relevance unknown'))

    for clase in set(val['Clase tipologia']):
        df_aux = val[val['Clase tipologia'] == clase]

        total = df_aux.shape[0]

        cantidad_alta = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        cantidad_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        cantidad_baja = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        duplicados_alta = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        duplicados_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        duplicados_baja = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == BAD_LEVEL]).shape[0] / total), 3)
        duplicados_desconocida = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == NO_LEVEL]).shape[0] / total), 3)

        completitud_alta = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        completitud_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        completitud_baja = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        nivel_informacion_alto = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        nivel_informacion_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        nivel_informacion_bajo = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        veracidad_alta = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        veracidad_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        veracidad_baja = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        veracidad_desconocida_alta = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        veracidad_desconocida_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        veracidad_desconocida_baja = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        severidad_alta = round((df_aux['Relevancia alta normalizada'].sum() / total), 3)
        severidad_media = round((df_aux['Relevancia media normalizada'].sum() / total), 3)
        severidad_baja = round((df_aux['Relevancia baja normalizada'].sum() / total), 3)
        severidad_desconocida = round((df_aux['Relevancia desconocida normalizada'].sum() / total), 3)

        val_categorias_evento = val_categorias_evento.append({'Event class': clase,
                                                              'Quantity high': cantidad_alta,
                                                              'Quantity acceptable': cantidad_aceptable,
                                                              'Quantity low': cantidad_baja,
                                                              'Duplicates high': duplicados_alta,
                                                              'Duplicates acceptable': duplicados_aceptable,
                                                              'Duplicates low': duplicados_baja,
                                                              'Duplicates unknown': duplicados_desconocida,
                                                              'Completeness high': completitud_alta,
                                                              'Completeness acceptable': completitud_aceptable,
                                                              'Completeness low': completitud_baja,
                                                              'Information level high': nivel_informacion_alto,
                                                              'Information level acceptable': nivel_informacion_aceptable,
                                                              'Information level low': nivel_informacion_bajo,
                                                              'Veracity high': veracidad_alta,
                                                              'Veracity acceptable': veracidad_aceptable,
                                                              'Veracity low': veracidad_baja,
                                                              'Unknown veracity high': veracidad_desconocida_alta,
                                                              'Unknown veracity acceptable': veracidad_desconocida_aceptable,
                                                              'Unknown veracity low': veracidad_desconocida_baja,
                                                              'Relevance high': severidad_alta,
                                                              'Relevance medium': severidad_media,
                                                              'Relevance low': severidad_baja,
                                                              'Relevance unknown': severidad_desconocida
                                                              }, ignore_index=True)


    return val_categorias_evento



####################################################################################################
def valorar_calidad_por_tipo_evento(val):
    """
    Gets data for the "total quality (max, avg and min) by event class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_categorias_evento: pandas dataframe
                           Data source evaluation structure
    """

    val_eventos_tipo = pd.DataFrame(columns=('Event class code',
                                             'Event class',
                                             'Highest quality',
                                             'Average quality',
                                             'Lowest quality'))

    df_aux = val[['Clase tipologia', 'Calidad']]

    for clase in set(df_aux['Clase tipologia']):
        df_aux_clase = df_aux[df_aux['Clase tipologia'] == clase]

        maximo = round((df_aux_clase['Calidad'].max()), 3)
        promedio = round((df_aux_clase['Calidad'].mean()), 3)
        minimo = round((df_aux_clase['Calidad'].min()), 3)

        val_eventos_tipo = val_eventos_tipo.append({'Event class code': clase,
                                                    'Event class': DICT_CLASES_EVENTOS[clase],
                                                    'Highest quality': maximo,
                                                    'Average quality': promedio,
                                                    'Lowest quality': minimo
                                                    }, ignore_index=True)


    return val_eventos_tipo



####################################################################################################
def valorar_calidad_por_tipo_fuente(val_fuentes):
    """
    Evaluates datasource global quality level in each type (own, public and private).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_fuentes_tipo: pandas dataframe
                      Data source evaluation structure
    """

    val_fuentes_tipo = pd.DataFrame(columns=('Data source type',
                                             'Highest quality',
                                             'Average quality',
                                             'Lowest quality'))

    df_aux = val_fuentes[['Tipo', 'Calidad', 'Diversidad']]
    df_aux['Total'] = df_aux['Calidad'] + df_aux['Diversidad']

    for tipo in set(df_aux['Tipo']):
        df_aux_tipo = df_aux[df_aux['Tipo'] == tipo]

        maximo = round((df_aux_tipo["Total"].max()), 3)
        promedio = round((df_aux_tipo['Total'].mean()), 3)
        minimo = round((df_aux_tipo["Total"].min()), 3)

        val_fuentes_tipo = val_fuentes_tipo.append({'Data source type' : tipo,
                                                    'Highest quality' : maximo,
                                                    'Average quality' : promedio,
                                                    'Lowest quality' : minimo
                                                    }, ignore_index=True)


    return val_fuentes_tipo



####################################################################################################
def valorar_calidad_fuente_por_dimension(val):
    """
    Gets data for the "data source class quality in each dimension" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_fuentes_dimension: pandas dataframe
                           Data source evaluation structure
    """

    val_fuentes_dimension = pd.DataFrame(columns=('Quality dimension',
                                                  'Data source class',
                                                  'High quality',
                                                  'Acceptable quality',
                                                  'Low quality'))

    for tipo in set(val['Data source type']):
        df_aux = val[val['Data source type'] == tipo]

        total = df_aux.shape[0]

        cantidad_alta = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        cantidad_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        cantidad_baja = round(((df_aux.loc[df_aux.loc[:, 'Cantidad nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        duplicados_alta = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        duplicados_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        duplicados_baja = round(((df_aux.loc[df_aux.loc[:, 'Duplicados nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        completitud_alta = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        completitud_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        completitud_baja = round(((df_aux.loc[df_aux.loc[:, 'Completitud nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        nivel_informacion_alto = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        nivel_informacion_aceptabe = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        nivel_informacion_bajo = round(((df_aux.loc[df_aux.loc[:, 'Nivel de informacion nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        veracidad_alta = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        veracidad_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        veracidad_baja = round(((df_aux.loc[df_aux.loc[:, 'Veracidad nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        veracidad_desconocida_alta = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == GOOD_LEVEL]).shape[0] / total), 3)
        veracidad_desconocida_aceptable = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == ACCEPTABLE_LEVEL]).shape[0] / total), 3)
        veracidad_desconocida_baja = round(((df_aux.loc[df_aux.loc[:, 'Veracidad desconocida nivel'] == BAD_LEVEL]).shape[0] / total), 3)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Quantity',
                                                              'Data source class': tipo,
                                                              'High quality': cantidad_alta,
                                                              'Acceptable quality': cantidad_aceptable,
                                                              'Low quality': cantidad_baja
                                                              }, ignore_index=True)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Duplicity',
                                                              'Data source class': tipo,
                                                              'High quality': duplicados_alta,
                                                              'Acceptable quality': duplicados_aceptable,
                                                              'Low quality': duplicados_baja
                                                              }, ignore_index=True)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Completeness',
                                                              'Data source class': tipo,
                                                              'High quality': completitud_alta,
                                                              'Acceptable quality': completitud_aceptable,
                                                              'Low quality': completitud_baja
                                                              }, ignore_index=True)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Information level',
                                                              'Data source class': tipo,
                                                              'High quality': nivel_informacion_alto,
                                                              'Acceptable quality': nivel_informacion_aceptabe,
                                                              'Low quality': nivel_informacion_bajo
                                                              }, ignore_index=True)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Veracity',
                                                              'Data source class': tipo,
                                                              'High quality': veracidad_alta,
                                                              'Acceptable quality': veracidad_aceptable,
                                                              'Low quality': veracidad_baja
                                                              }, ignore_index=True)

        val_fuentes_dimension = val_fuentes_dimension.append({'Quality dimension': 'Unknown veracity',
                                                              'Data source class': tipo,
                                                              'High quality': veracidad_desconocida_alta,
                                                              'Acceptable quality': veracidad_desconocida_aceptable,
                                                              'Low quality': veracidad_desconocida_baja
                                                              }, ignore_index=True)


    return val_fuentes_dimension



####################################################################################################
def valorar_eventos_cubiertos_por_fuentes(val):
    """
    Gets data for the "events covered by data source class" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_eventos_cubiertos_por_fuentes: pandas dataframe
                                       Structure with events covered by data source class
    """

    val_eventos_cubiertos_por_fuentes = pd.DataFrame(columns=('Event class code',
                                                              'Event class',
                                                              'Event typology',
                                                              'Own data sources',
                                                              'Public data sources',
                                                              'Private data sources'))

    for tipologia in set(val['Tipologia']):
        df_aux = val[val['Tipologia'] == tipologia]
        clase = df_aux.loc[df_aux.index[0], 'Clase tipologia']
        propias = df_aux[df_aux['Data source type'] == 'Own'].shape[0]
        publicas = df_aux[df_aux['Data source type'] == 'Public'].shape[0]
        privadas = df_aux[df_aux['Data source type'] == 'Private'].shape[0]

        val_eventos_cubiertos_por_fuentes = val_eventos_cubiertos_por_fuentes.append({'Event class code': clase,
                                                                                      'Event class': DICT_CLASES_EVENTOS[clase],
                                                                                      'Event typology': tipologia,
                                                                                      'Own data sources': propias,
                                                                                      'Public data sources': publicas,
                                                                                      'Private data sources': privadas
                                                                                      }, ignore_index=True)

    val_eventos_cubiertos_por_fuentes['Own data sources'] = val_eventos_cubiertos_por_fuentes['Own data sources'].apply(lambda x: "No" if x == 0 else "Yes")
    val_eventos_cubiertos_por_fuentes['Public data sources'] = val_eventos_cubiertos_por_fuentes['Public data sources'].apply(lambda x: "No" if x == 0 else "Yes")
    val_eventos_cubiertos_por_fuentes['Private data sources'] = val_eventos_cubiertos_por_fuentes['Private data sources'].apply(lambda x: "No" if x == 0 else "Yes")


    return val_eventos_cubiertos_por_fuentes



####################################################################################################
def valorar_datos_por_fuente(val):
    """
    Gets data for the "number of data in each source" report.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    val_datos_por_fuente : pandas dataframe
                           Data source evaluation structure
    """

    g = val.groupby(['Data source'])['Cantidad'].sum()
    val_datos_por_fuente = g.reset_index()


    return val_datos_por_fuente



####################################################################################################
#                                                                                                  #
#   Analytic Hierarchy Process (AHP)                                                               #
#                                                                                                  #
####################################################################################################
#                                                                                                  #
# Code adapted from J. Papathanasiou, N. Ploskas, Multiple Criteria Decision Aid, Springer         #
# Optimization and Its Applications 136 (2018)                                                     #
#                                                                                                  #
####################################################################################################

def norm(x):
    """
    Normalized column sum method.
    Source code adapted from J. Papathanasiou 2018.

    Parameters
    ----------
    x: numpy array
       Pairwise comparison matrix for the criteria or the alternatives

    Returns
    -------
    z: numpy array
       Normalized pairwise comparison matrix for the criteria or the alternatives
    """

    k = np.array(sum(x, 0))
    z = np.array([[round(x[i, j] / k[j], 3)
                   for j in range(x.shape[1])]
                  for i in range(x.shape[0])])


    return z



####################################################################################################
def geomean(x):
    """
    Geometric mean method.
    Source code adapted from J. Papathanasiou 2018.

    Paramenters
    -----------
    x: numpy array
       Pairwise comparison matrix for the criteria or the alternatives

    Returns
    -------
    z: numpy array
       Geometric mean of pairwise comparison matrix for the criteria or the alternatives
    """

    z = [1] * x.shape[0]
    for i in range(x.shape[0]):
        for j in range(x.shape[1]):
            z[i] = z[i] * x[i][j]
        z[i] = pow(z[i], (1 / x.shape[0]))


    return z



####################################################################################################
def ahp(PCM, PCcriteria, m, n, c):
    """
    AHP method: it calls the other functions.
    Source code adapted from J. Papathanasiou 2018.

    Parameters
    ----------
    PCM: numpy array
         Pairwise comparison matrix for the alternatives
    PCcriteria: numpy array
                Pairwise comparison matrix for the criteria
    m: int
       Number of the alternatives
    n: int
       Number of the criteria
    c: int
       Method to estimate a priority vector (1 for eigenvector, 2 for normalized column sum, and 3
       for geometric mean)

    Returns
    -------
    v: numpy array
       Global priority vector for the alternatives
    """

    # calculate the priority vector of criteria
    if c == 1: # eigenvector
        val, vec = sc.eigs(PCcriteria, k=1, which='LM')
        eigcriteria = np.real(vec)
        w = eigcriteria / sum(eigcriteria)
        w = np.array(w).ravel()
    elif c == 2: # normalized column sum
        normPCcriteria = norm(PCcriteria)
        w = np.array(sum(normPCcriteria, 1) / n)
    else: # geometric mean
        GMcriteria = geomean(PCcriteria)
        w = GMcriteria / sum(GMcriteria)

    # calculate the local priority vectors for the alternatives
    S = []
    for i in range(n):
        if c == 1: # eigenvector
            val, vec = sc.eigs(PCM[i * m:i * m + m, 0:m], k=1, which='LM')

            # Control para matrices 2x2
            if vec.shape[1] > 1:
                vec = vec[:, np.argmax(val)]

            eigalter = np.real(vec)
            s = eigalter / sum(eigalter)
            s = np.array(s).ravel()
        elif c == 2: # normalized column sum
            normPCM = norm(PCM[i*m:i*m+m, 0:m])
            s = np.array(sum(normPCM, 1) / m)
        else: # geometric mean
            GMalternatives = geomean(PCM[i*m:i*m+m, 0:m])
            s = GMalternatives / sum(GMalternatives)

        S.append(s)
    S = np.transpose(S)

    # calculate the global priority vector for the alternatives
    v = S.dot(w.T)


    return v



####################################################################################################
def ejecutar_ahp(val, PCcriteria, s_m, c, tip):
    """
    Runs AHP process.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    PCcriteria: numpy array
                Pairwise comparison matrix for the criteria
    s_m: pandas dataframe
         Score matrix with AHP levels
    c: int
       Method to estimate a priority vector (1 for eigenvector, 2 for normalized column sum, and 3
       for geometric mean)
    tip: string
         Event typology

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """

    s_m.reset_index(inplace=True)

    lista_fuentes = s_m['Data source'].to_list()

    # the number of the alternatives
    m = s_m.shape[0]

    # the number of the criteria
    n = 6

    # random indices for consistency checking
    RI = [0, 0, 0.58, 0.90, 1.12, 1.24, 1.32, 1.41, 1.45, 1.49]

    # pairwise comparison matrix of the alternatives
    lista_criterios = s_m.columns.to_list()
    lista_criterios.remove('Data source')
    lista_criterios.remove('index')

    allPCM = np.matrix(np.empty(shape=(0, m), dtype=float))
    for criterio in lista_criterios:
        PCM = np.matrix(np.empty(shape=(0, m), dtype=float))
        for i in range(len(s_m)):
            array = []
            for j in range(len(s_m)):
                Fi = s_m.loc[i, criterio]
                Fj = s_m.loc[j, criterio]
                if Fi == Fj:
                    value = 1
                elif (Fi == 'N' and Fj == 'R') or (Fi == 'V' and Fj == 'A'):
                    value = 5
                elif (Fi == 'A' and Fj == 'N'):
                    value = 3
                elif (Fi == 'A' and Fj == 'R') or (Fi == 'V' and Fj == 'N'):
                    value = 7
                elif (Fi == 'V' and Fj == 'R'):
                    value = 9
                elif (Fj == 'N' and Fi == 'R') or (Fj == 'V' and Fi == 'A'):
                    value = 1/5
                elif (Fj == 'A' and Fi == 'N'):
                    value = 1/3
                elif (Fj == 'A' and Fi == 'R') or (Fj == 'V' and Fi == 'N'):
                    value = 1/7
                elif (Fj == 'V' and Fi == 'R'):
                    value = 1/9
                elif (Fi == '0' or Fj == '0'):
                    value = 1
                else:
                    value = 0

                array = np.append(array, value)

            PCM = np.insert(PCM, i, array, axis=0)

        allPCM = np.vstack((allPCM, PCM))

    # consistency check for pairwise comparison matrix of the alternatives
    print("Inconsistency indexes for typology ", tip, ":", sep="")
    for i in range(n):
        lambdamax = np.amax(np.linalg.eigvals(allPCM[i * m:i * m + m, 0:m]).real)
        CI = (lambdamax - m) / (m - 1)
        CR = CI / RI[m - 1]
        print("Inconsistency index of the alternatives for criterion ",
              CRITERIOS_AHP[i],
              ": ",
              CR,
              sep="")
        if CR > 0.1:
            print("The pairwise comparison matrix of the alternatives for criterion ",
                  CRITERIOS_AHP[i],
                  " is inconsistent",
                  sep="")

    # call ahp method
    scores = ahp(allPCM, PCcriteria, m, n, c)

    for i in range(len(scores)):
        score = round(scores[i], 3)
        val.loc[(val.loc[:, 'Tipologia'] == tip) & (val.loc[:, 'Data source'] == lista_fuentes[i]), 'AHP score'] = score


    return val



####################################################################################################
def generar_matrices_AHP(val, PCcriteria):
    """
    Evaluate AHP levels for all event typologies provided by more than one data source.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    PCcriteria: numpy array
                Pairwise comparison matrix for the criteria

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    """
    for tipologia in set(val['Tipologia']):
        df_val_aux = val[val['Tipologia'] == tipologia]
        if df_val_aux.shape[0] > 1:
            score_matrix = df_val_aux[['Data source',
                                       'Cantidad AHP',
                                       'Duplicados AHP',
                                       'Completitud AHP',
                                       'Nivel de informacion AHP',
                                       'Veracidad AHP',
                                       'Veracidad desconocida AHP']]

            val = ejecutar_ahp(val, PCcriteria, score_matrix, 1, tipologia)


    return val



####################################################################################################
#                                                                                                  #
#   GENERACION DE INFORMES                                                                         #
#                                                                                                  #
####################################################################################################
def encode_image(path_to_image):
    """
    Encode image.

    Paramenters
    -----------
    path_to_image: string
                   Path to image file

    Returns
    -------
    None
    """

    with open(path_to_image, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    print(type("data:image/png;base64,"), type(encoded_string))
    print(b'data:image/png;base64,' + encoded_string)


    return b'data:image/png;base64,' + encoded_string



####################################################################################################
def crear_report_fuentes(path, tit, fue_dat, lista_cabecera, df_fuente_obs, df_raw_data, df_quality_data, df_normalized_data, lista_valoracion):
    """
    Generates data sources reports in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    tit: string
         Report title
    fue_dat: string
             Data source name
    lista_cabecera: list
                    Header attributes
    df_fuente_obs: pandas dataframe
                   Header values
    df_raw_data: pandas dataframe
                 Raw data for data source
    df_quality_data: pandas dataframe
                     Quality evaluation for data source
    df_normalized_data: pandas dataframe
                        Raw data for data source
    lista_valoracion: list
                      Quality attributes

    Returns
    -------
    None
    """
    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": tit,
                     "sufijo_title": fue_dat,
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    # Datos globales de la fuente
    tabla_formateada = "<h3>Data source global information:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    tabla_formateada += "<tr>"
    for atributo in lista_cabecera:
        if atributo == 'Tipo':
            atributo = 'Source type'
        elif atributo == 'Tipologias':
            atributo = 'Event tipologies'
        elif atributo == 'Valoracion datos obsoletos':
            atributo = 'Obsolete data valuation'
        elif atributo == 'Tasa falsos positivos':
            atributo = 'False positive rate'
        elif atributo == 'Precio':
            atributo = 'Price'
        elif atributo == 'Valoracion manual':
            atributo = 'Manual evaluation'

        tabla_formateada += "<td align='center' class='black'><strong>" + str(atributo) + "</strong></td>"

    tabla_formateada += "</tr><tr>"
    for atributo in lista_cabecera:
        tabla_formateada += "<td align='center'>" + (str(df_fuente_obs[atributo].values[0])).replace('.', ',') + "</td>"
    tabla_formateada += "</tr></table>"


    # Datos en crudo
    tabla_formateada += "<br/><h3>Raw data, by event typology:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_raw_data.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_raw_data)):
        obs = df_raw_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Datos normalizados
    tabla_formateada += "<br/><h3>Normalized data, by event typology:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    nombres_columnas_datos_normalizados = df_normalized_data.columns
    lista_atributos_normalizados = []
    lista_nombres_niveles = []

    for elemento in nombres_columnas_datos_normalizados:
        if 'nivel' not in elemento:
            lista_atributos_normalizados.append(elemento)
        else:
            lista_nombres_niveles.append(elemento)

    tabla_formateada += "<tr>"

    # Imprimimos cabecera
    for elemento in lista_atributos_normalizados:
        if elemento == EVENT_TYPOLOGY:
            elemento = 'Event typology'
        elif elemento == 'Cantidad normalizada':
            elemento = 'Quantity'
        elif elemento == 'Duplicados normalizada':
            elemento = 'Duplicity'
        elif elemento == 'Completitud normalizada':
            elemento = 'Completeness'
        elif elemento == 'Nivel de informacion normalizada':
            elemento = 'Information level'
        elif elemento == 'Veracidad normalizada':
            elemento = 'Veracity'
        elif elemento == 'Veracidad desconocida normalizada':
            elemento = 'Unknown veracity'
        elif elemento == 'Frecuencia normalizada':
            elemento = 'Frecuency'
        elif elemento == 'Consistencia normalizada':
            elemento = 'Consistency'
        elif elemento == 'Relevancia alta normalizada':
            elemento = 'High severity'
        elif elemento == 'Relevancia media normalizada':
            elemento = 'Medium severity'
        elif elemento == 'Relevancia baja normalizada':
            elemento = 'Low severity'
        elif elemento == 'Relevancia desconocida normalizada':
            elemento = 'Unknown severity'
        elif elemento == 'Precio por dato normalizada':
            elemento = 'Price per data'

        tabla_formateada += "<td align='center' class='black letra ancho'>" + elemento + "</td>"

    tabla_formateada += "</tr>"

    # Imprimimos datos normalizados
    for ind in range(len(df_normalized_data)):
        tabla_formateada += "<tr>"
        obs = df_normalized_data.iloc[ind:ind+1]

        for nombre_atributo in lista_atributos_normalizados:
            valor = obs[nombre_atributo].values[0]
            nombre_atributo_nivel = nombre_atributo.replace('normalizada', 'nivel')
            atributo_nivel = ''
            if nombre_atributo_nivel in lista_nombres_niveles:
                atributo_nivel = obs[nombre_atributo_nivel].values[0]
                if atributo_nivel == 1:
                    tabla_formateada += "<td class='bad' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 2:
                    tabla_formateada += "<td class='acceptable' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 3:
                    tabla_formateada += "<td class='good' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 0:
                    tabla_formateada += "<td align='center'>" + (str(valor)).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + str(valor) + "</td>"

        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    # Valoracion por tipologia
    tabla_formateada += "<br/><h3>Evaluation, by event typology:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_quality_data.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_quality_data)):
        obs = df_quality_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Valoracion final
    tabla_formateada += "<br/><h3>Final evaluation:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'>"
    tabla_formateada += "<tr>"
    for atributo in lista_valoracion:
        if atributo == 'Calidad':
            atributo = 'Quality'
        elif atributo == 'Diversidad':
            atributo = 'Diversity'

        tabla_formateada += "<td align='center' class='black'><strong>" + str(atributo) + "</strong></td>"
    tabla_formateada += "</tr><tr>"
    for atributo in lista_valoracion:
        tabla_formateada += "<td align='center'>" + (str(round(df_fuente_obs[atributo].values[0], 3))).replace('.', ',') + "</td>"
    tabla_formateada += "</tr></table>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Report_data_source_"+fue_dat+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_fuentes(val, val_fuentes):
    """
    Generates data for data sources reports.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    val_fuentes: pandas dataframe
                 Data source evaluation structure

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)
    for i in range(len(val_fuentes)):
        obs = val_fuentes.iloc[i:i+1]
        vendor = obs[FIELD_DATA_SOURCE].values[0]
        df_tipologia_vendor = val[val['Data source'] == vendor]
        df_raw_typology_data = df_tipologia_vendor[['Tipologia',
                                                    'Cantidad',
                                                    'Duplicados',
                                                    'Completitud',
                                                    'Nivel de informacion',
                                                    'Veracidad',
                                                    'Veracidad desconocida',
                                                    'Frecuencia',
                                                    'Consistencia',
                                                    'Relevancia alta',
                                                    'Relevancia media',
                                                    'Relevancia baja',
                                                    'Relevancia desconocida',
                                                    'Precio por dato']]
        df_raw_typology_data.rename(columns={'Tipologia': 'Event typology',
                                             'Cantidad': 'Quantity',
                                             'Duplicados': 'Duplicity',
                                             'Completitud': 'Completeness',
                                             'Nivel de informacion': 'Information level',
                                             'Veracidad': 'Veracity',
                                             'Veracidad desconocida': 'Unknown veracity',
                                             'Frecuencia': 'Frecuency',
                                             'Consistencia': 'Consistency',
                                             'Relevancia alta': 'High severity',
                                             'Relevancia media': 'Medium severity',
                                             'Relevancia baja': 'Low severity',
                                             'Relevancia desconocida': 'Unknown severity',
                                             'Precio por dato': 'Price per data'
                                            }, inplace=True
                                    )

        df_normalized_typology = df_tipologia_vendor[['Tipologia',
                                                      'Cantidad normalizada',
                                                      'Duplicados normalizada',
                                                      'Completitud normalizada',
                                                      'Nivel de informacion normalizada',
                                                      'Veracidad normalizada',
                                                      'Veracidad desconocida normalizada',
                                                      'Frecuencia normalizada',
                                                      'Consistencia normalizada',
                                                      'Relevancia alta normalizada',
                                                      'Relevancia media normalizada',
                                                      'Relevancia baja normalizada',
                                                      'Relevancia desconocida normalizada',
                                                      'Precio por dato normalizada',
                                                      'Cantidad nivel',
                                                      'Duplicados nivel',
                                                      'Completitud nivel',
                                                      'Nivel de informacion nivel',
                                                      'Veracidad nivel',
                                                      'Veracidad desconocida nivel',
                                                      'Frecuencia nivel',
                                                      'Consistencia nivel',
                                                      'Precio por dato nivel']]


        df_quality_typology = df_tipologia_vendor[['Tipologia', 'Calidad', 'Exclusividad']]
        df_quality_typology.rename(columns={'Tipologia': 'Event typology',
                                            'Calidad': 'Quality',
                                            'Exclusividad': 'Exclusivity'}, inplace=True,
                                   )

        crear_report_fuentes(path_to_output,
                             TITULO_FUENTES,
                             vendor,
                             ['Tipo',
                              'Tipologias',
                              'Valoracion datos obsoletos',
                              'Tasa falsos positivos',
                              'Precio',
                              'Valoracion manual'],
                             obs,
                             df_raw_typology_data,
                             df_quality_typology,
                             df_normalized_typology,
                             ['Calidad',
                              'Diversidad',
                              'Total']
                            )



####################################################################################################
def crear_report_tipologias(path, tit, tip, df_raw_data, df_normalized_data, df_quality_data, df_ahp_typology):
    """
    Generates event typologies reports in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    tit: string
         Report title
    tip: string
         Event typology name
    df_raw_data: pandas dataframe
                 Raw data for event typology
    df_normalized_data: pandas dataframe
                        Raw data for event typology
    df_quality_data: pandas dataframe
                     Quality evaluation for event typology
    df_ahp_typology: pandas dataframe
                     AHP evaluation for event typology

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": tit,
                     "sufijo_title": tip,
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    # Datos en crudo
    tabla_formateada = "<br/><h3>Raw data, by data source:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_raw_data.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_raw_data)):
        obs = df_raw_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Datos normalizados
    tabla_formateada += "<br/><h3>Normalized data, by data source:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    nombres_columnas_datos_normalizados = df_normalized_data.columns
    lista_atributos_normalizados = []
    lista_nombres_niveles = []

    for elemento in nombres_columnas_datos_normalizados:
        if 'nivel' not in elemento:
            lista_atributos_normalizados.append(elemento)
        else:
            lista_nombres_niveles.append(elemento)

    tabla_formateada += "<tr>"

    # Imprimimos cabecera
    for elemento in lista_atributos_normalizados:
        if elemento == DATA_SOURCE:
            elemento = 'Data source'
        elif elemento == 'Cantidad normalizada':
            elemento = 'Quantity'
        elif elemento == 'Duplicados normalizada':
            elemento = 'Duplicity'
        elif elemento == 'Completitud normalizada':
            elemento = 'Completeness'
        elif elemento == 'Nivel de informacion normalizada':
            elemento = 'Information level'
        elif elemento == 'Veracidad normalizada':
            elemento = 'Veracity'
        elif elemento == 'Veracidad desconocida normalizada':
            elemento = 'Unknown veracity'
        elif elemento == 'Frecuencia normalizada':
            elemento = 'Frecuency'
        elif elemento == 'Consistencia normalizada':
            elemento = 'Consistency'
        elif elemento == 'Relevancia alta normalizada':
            elemento = 'High severity'
        elif elemento == 'Relevancia media normalizada':
            elemento = 'Medium severity'
        elif elemento == 'Relevancia baja normalizada':
            elemento = 'Low severity'
        elif elemento == 'Relevancia desconocida normalizada':
            elemento = 'Unknown severity'
        elif elemento == 'Precio por dato normalizada':
            elemento = 'Price per data'

        tabla_formateada += "<td align='center' class='black letra ancho'>" + elemento + "</td>"

    tabla_formateada += "</tr>"

    # Imprimimos datos normalizados
    for ind in range(len(df_normalized_data)):
        tabla_formateada += "<tr>"
        obs = df_normalized_data.iloc[ind:ind+1]

        for nombre_atributo in lista_atributos_normalizados:
            valor = obs[nombre_atributo].values[0]
            nombre_atributo_nivel = nombre_atributo.replace('normalizada', 'nivel')
            atributo_nivel = ''
            if nombre_atributo_nivel in lista_nombres_niveles:
                atributo_nivel = obs[nombre_atributo_nivel].values[0]
                if atributo_nivel == 1:
                    tabla_formateada += "<td class='bad' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 2:
                    tabla_formateada += "<td class='acceptable' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 3:
                    tabla_formateada += "<td class='good' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 0:
                    tabla_formateada += "<td align='center'>" + (str(valor)).replace('.', ',') + "</td>"
            else:
                if nombre_atributo_nivel == DATA_SOURCE:
                    tabla_formateada += "<td align='center'>" + str(valor) + "</td>"
                else:
                    tabla_formateada += "<td align='center'>" + str(valor) + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    # Clasificacion por calidad
    tabla_formateada += "<br/><h3>Data source ranking:</h3>"
    tabla_formateada += "<table align='center' width='30%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_quality_data.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_quality_data)):
        obs = df_quality_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == DATA_SOURCE:
                #tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Clasificacion AHP
    tabla_formateada += "<br/><h3>AHP ranking:</h3>"
    tabla_formateada += "<table align='center' width='30%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_ahp_typology.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_ahp_typology)):
        obs = df_ahp_typology.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Report_event_typology_"+tip+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_tipologias(val):
    """
    Generates data for event typologies reports.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    for tipologia in set(val['Tipologia']):
        df_tipologia = val[val['Tipologia'] == tipologia]
        df_tipologia.sort_values(['Tipologia', 'Calidad', 'Data source'],
                                 ascending=[True, False, True],
                                 inplace=True)
        df_raw_typology_data = df_tipologia[['Data source',
                                             'Cantidad',
                                             'Duplicados',
                                             'Completitud',
                                             'Nivel de informacion',
                                             'Veracidad',
                                             'Veracidad desconocida',
                                             'Frecuencia',
                                             'Consistencia',
                                             'Relevancia alta',
                                             'Relevancia media',
                                             'Relevancia baja',
                                             'Relevancia desconocida',
                                             'Precio por dato']]
        df_raw_typology_data.rename(columns={'Data source': 'Data source',
                                             'Cantidad': 'Quantity',
                                             'Duplicados': 'Duplicity',
                                             'Completitud': 'Completeness',
                                             'Nivel de informacion': 'Information level',
                                             'Veracidad': 'Veracity',
                                             'Veracidad desconocida': 'Unknown veracity',
                                             'Frecuencia': 'Frecuency',
                                             'Consistencia': 'Consistency',
                                             'Relevancia alta': 'High severity',
                                             'Relevancia media': 'Medium severity',
                                             'Relevancia baja': 'Low severity',
                                             'Relevancia desconocida': 'Unknown severity',
                                             'Precio por dato': 'Price per data'
                                            }, inplace=True
                                    )
        df_normalized_typology = df_tipologia[['Data source',
                                               'Cantidad normalizada',
                                               'Duplicados normalizada',
                                               'Completitud normalizada',
                                               'Nivel de informacion normalizada',
                                               'Veracidad normalizada',
                                               'Veracidad desconocida normalizada',
                                               'Frecuencia normalizada',
                                               'Consistencia normalizada',
                                               'Relevancia alta normalizada',
                                               'Relevancia media normalizada',
                                               'Relevancia baja normalizada',
                                               'Relevancia desconocida normalizada',
                                               'Precio por dato normalizada',
                                               'Cantidad nivel',
                                               'Duplicados nivel',
                                               'Completitud nivel',
                                               'Nivel de informacion nivel',
                                               'Veracidad nivel',
                                               'Veracidad desconocida nivel',
                                               'Frecuencia nivel',
                                               'Consistencia nivel',
                                               'Precio por dato nivel']]

        df_quality_typology = df_tipologia[['Data source', 'Calidad']]
        df_quality_typology.sort_values(['Calidad', 'Data source'],
                                        ascending=[False, True],
                                        inplace=True)
        df_quality_typology.rename(columns={'Data source': 'Data source',
                                            'Calidad': 'Quality'
                                            }, inplace=True
                                  )

        df_ahp_typology = df_tipologia[['Data source', 'AHP score']]
        df_ahp_typology.sort_values(['AHP score', 'Data source'],
                                    ascending=[False, True],
                                    inplace=True)

        crear_report_tipologias(path_to_output,
                                TITULO_TIPOLOGIAS,
                                tipologia,
                                df_raw_typology_data,
                                df_normalized_typology,
                                df_quality_typology,
                                df_ahp_typology)



####################################################################################################
def crear_report_ranking(path, titulo, df_val_fuentes, df_pesos, df_clasificacion, dict_gamm, df_pccriteria):
    """
    Generates datasource ranking report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_fuentes: pandas dataframe
                    Data source evaluation structure
    df_pesos: pandas dataframe
              Quality dimension evaluation weights
    df_clasificacion: pandas dataframe
                      Scores for data source classification
    df_pccriteria: pandas dataframe
                   Pairwise comparison matrix for the criteria

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '\"')
                    }

    # Clasificacion por calidad
    tabla_formateada = "<br/><h3>Data source ranking:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_val_fuentes.columns:
        if nombre_atributo == FIELD_DATA_SOURCE:
            nombre_atributo = FUENTE
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
        else:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_val_fuentes)):
        obs = df_val_fuentes.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == FIELD_DATA_SOURCE:
                #tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Clasificiones AHP
    for tipologia in sorted(set(df_clasificacion['Event typology'])):
        df_aux = df_clasificacion[df_clasificacion['Event typology'] == tipologia]

        df_riasc = df_aux[['Data source', 'Quality']]
        df_riasc.sort_values(['Quality', 'Data source'],
                             ascending=[False, True],
                             inplace=True)

        tabla_formateada += "<br/><h3>Quality ranking for event typology " + tipologia + ":</h3>"
        tabla_formateada += "<table align='center' width='40%' border='1' cellspacing='0' cellpadding='2'><tr>"
        for nombre_atributo in df_riasc.columns:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
        tabla_formateada += "</tr>"
        for ind in range(len(df_riasc)):
            obs = df_riasc.iloc[ind:ind+1]
            tabla_formateada += "<tr>"
            for atributo in obs.columns:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            tabla_formateada += "</tr>"
        tabla_formateada += "</table>"

        df_ahp = df_aux[['Data source', 'AHP score']]
        df_ahp.sort_values(['AHP score', 'Data source'],
                           ascending=[False, True],
                           inplace=True)

        tabla_formateada += "<br/><h3>AHP ranking for event typology " + tipologia + ":</h3>"
        tabla_formateada += "<table align='center' width='40%' border='1' cellspacing='0' cellpadding='2'><tr>"
        for nombre_atributo in df_ahp.columns:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
        tabla_formateada += "</tr>"
        for ind in range(len(df_ahp)):
            obs = df_ahp.iloc[ind:ind+1]
            tabla_formateada += "<tr>"
            for atributo in obs.columns:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            tabla_formateada += "</tr>"
        tabla_formateada += "</table>"

        gamma = dict_gamm[tipologia]
        gamma = str(gamma)
        tabla_formateada += "<br/><h3>Goodman-Kruskal gamma for event typology " + tipologia + ": " + gamma + "</h3>"

    # Indicacion de pesos utilizados en la valoracion de dimensiones de calidad
    tabla_formateada += "<br/><h4><i>Weights of score levels used to compile the ranking:</i></h4>"
    tabla_formateada += "<table align='left' width='40%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_pesos.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_pesos)):
        obs = df_pesos.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
    tabla_formateada += "</table>"

    # Indicacion de matriz de pesos utilizados en la valoracion ahp
    tabla_formateada += "<br/><h4><i>Pairwise comparison matrix of the AHP criteria, used to compile AHP rankings:</i></h4>"
    tabla_formateada += "<table align='left' width='50%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_pccriteria.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_pccriteria)):
        obs = df_pccriteria.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        # for atributo in obs.columns:
            # tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        for atributo in obs.columns:
            if obs[atributo].values[0] in CRITERIOS_AHP:
                tabla_formateada += "<td align='center' class='black letra ancho' >" + obs[atributo].values[0] + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"

    tabla_formateada += "</table>"


    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Data source ranking"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def goodman_kruskal_gamma(df_ranking_tip):
    """
    Calculates Goodman-Kruskal gammma.
    Source code adapted from https://bit.ly/3fZQ04L (https://PeterStatistics.com).

    Parameters
    ----------
    df_ranking_tip: pandas dataframe
                    Scores for one data source classification

    Returns
    -------
    gk_gamma: float
              Goodman-Kruskal gamma
    """

    ord_wsm = df_ranking_tip['wsm']
    ord_ahp = df_ranking_tip['ahp']

    crosstable = pd.crosstab(ord_wsm, ord_ahp)

    n_rows = crosstable.shape[0]
    n_cols = crosstable.shape[1]

    C = [[0 for x in range(n_cols)] for y in range(n_rows)]

    # top left part
    for i in range(n_rows):
        for j in range(n_cols):
            h = i-1
            k = j-1
            if h >= 0 and k >= 0:
                for p in range(h+1):
                    for q in range(k+1):
                        C[i][j] = C[i][j] + list(crosstable.iloc[p])[q]
    
    # bottom right part                    
    for i in range(n_rows):
        for j in range(n_cols):
            h = i+1
            k = j+1
            if h < n_rows and k < n_cols:
                for p in range(h, n_rows):
                    for q in range(k, n_cols):
                        C[i][j] = C[i][j] + list(crosstable.iloc[p])[q]

    D = [[0 for x in range(n_cols)] for y in range(n_rows)]

    # bottom left part
    for i in range(n_rows):
        for j in range(n_cols):
            h = i+1
            k = j-1
            if h < n_rows and k >= 0:
                for p in range(h, n_rows):
                    for q in range(k+1):
                        D[i][j] = D[i][j] + list(crosstable.iloc[p])[q]

    # top right part
    for i in range(n_rows):
        for j in range(n_cols):
            h = i-1
            k = j+1
            if h >= 0 and k < n_cols:
                for p in range(h+1):
                    for q in range(k, n_cols):
                        D[i][j] = D[i][j] + list(crosstable.iloc[p])[q]

    P = 0
    Q = 0
    for i in range(n_rows):
        for j in range(n_cols):
            P = P + C[i][j] * crosstable.iloc[i][j+1]
            Q = Q + D[i][j] * crosstable.iloc[i][j+1]

    gk_gamma = (P - Q) / (P + Q)

    if np.isnan(gk_gamma):
        gk_gamma = 1.0

    gk_gamma = round(gk_gamma, 3)


    return gk_gamma



####################################################################################################
def calcular_gammas(df_clasif):
    """
    Sets the wsm and ahp rankings, and call the Goodman-Kruskal gamma calculation function, for each
    event typology.

    Parameters
    ----------
    df_clasif: pandas_dataframe
               Scores for data source classification

    Returns
    -------
    dict_gamm: dictionary
               Goodman-Kruskal gamma for each event typology
    """


    dict_gamm = {}
    for tipologia in sorted(set(df_clasif['Event typology'])):
        df_ranking_tip = df_clasif[df_clasif['Event typology'] == tipologia]
        df_ranking_tip['wsm'] = 0
        df_ranking_tip['ahp'] = 0

        df_ranking_tip.sort_values(['Quality'], ascending=[False], inplace=True)
        df_ranking_tip.reset_index(drop=True, inplace=True)
        for i in range(len(df_ranking_tip)):
            if i == 0:
                df_ranking_tip.loc[i, 'wsm'] = 1
            elif df_ranking_tip.loc[i, 'Quality'] == df_ranking_tip.loc[i-1, 'Quality']:
                df_ranking_tip.loc[i, 'wsm'] = df_ranking_tip.loc[i-1, 'wsm']
            else:
                df_ranking_tip.loc[i, 'wsm'] = df_ranking_tip.loc[i-1, 'wsm'] + 1

        df_ranking_tip.sort_values(['AHP score'], ascending=[False], inplace=True)
        df_ranking_tip.reset_index(drop=True, inplace=True)
        for i in range(len(df_ranking_tip)):
            if i == 0:
                df_ranking_tip.loc[i, 'ahp'] = 1
            elif df_ranking_tip.loc[i, 'AHP score'] == df_ranking_tip.loc[i-1, 'AHP score']:
                df_ranking_tip.loc[i, 'ahp'] = df_ranking_tip.loc[i-1, 'ahp']
            else:
                df_ranking_tip.loc[i, 'ahp'] = df_ranking_tip.loc[i-1, 'ahp'] + 1

        df_ranking_tip.drop(['Event typology', 'Quality', 'AHP score'],
                            axis='columns',
                            inplace=True)

        gamma = goodman_kruskal_gamma(df_ranking_tip)
        dict_gamm[tipologia] = gamma


    return dict_gamm



####################################################################################################
def generar_informe_ranking(val_fuentes, val, PCcriteria):
    """
    Generates data for datasource ranking report.

    Parameters
    ----------
    val_fuentes: pandas dataframe
                 Data source evaluation structure
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology
    PCcriteria: numpy array
                Pairwise comparison matrix for the criteria

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_valoracion_fuentes = val_fuentes[[FIELD_DATA_SOURCE,
                                         'Tipo', 'Precio',
                                         'Calidad',
                                         'Diversidad',
                                         'Total']]
    df_valoracion_fuentes.sort_values(['Total', 'Calidad', 'Diversidad', FIELD_DATA_SOURCE],
                                      ascending=[False, False, False, True],
                                      inplace=True)

    df_valoracion_fuentes.rename(columns={FIELD_DATA_SOURCE: 'Data source',
                                          'Tipo': 'Type',
                                          'Precio': 'Price',
                                          'Calidad': 'Quality',
                                          'Diversidad': 'Diversity',
                                          'Total': 'Total'
                                          }, inplace=True,
                                 )

    df_pesos = pd.DataFrame(columns=['Good level (green)',
                                     'Acceptable level (yellow)',
                                     'Bad level (red)'])
    df_pesos.loc[0] = [W1, W2, W3]


    df_clasificacion = val[['Tipologia', 'Data source', 'Calidad', 'AHP score']]
    df_clasificacion = df_clasificacion.loc[df_clasificacion.loc[:, 'AHP score'] != 'N/A']
    df_clasificacion.rename(columns={'Tipologia': 'Event typology',
                                     'Calidad': 'Quality'
                                     }, inplace=True)


    dict_gammmas = calcular_gammas(df_clasificacion)

    columnas = ['CRITERIA']
    columnas += CRITERIOS_AHP
    df_pccriteria = pd.DataFrame(columns=columnas)
    for i in range(len(CRITERIOS_AHP)):
        linea = []
        linea += [CRITERIOS_AHP[i]]
        linea += PCcriteria[i,].tolist()
        df_pccriteria.loc[i] = linea

    for criterio in CRITERIOS_AHP:
        df_pccriteria[criterio] = df_pccriteria[criterio].apply(lambda x: round(x, 3))


    crear_report_ranking(path_to_output,
                         TITULO_RANKING,
                         df_valoracion_fuentes,
                         df_pesos,
                         df_clasificacion,
                         dict_gammmas,
                         df_pccriteria)



####################################################################################################
def crear_report_porcentaje_por_evento(path, titulo, df_val_porcentaje_por_evento):
    """
    Generates percentage of events report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_porcentaje_por_evento: pandas dataframe
                                  Percentage and list of events

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Types of cybersecuryty events according to Ministerio de Interior: Guia Nacional de Notificacion y Gestion de Ciberincidentes:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_porcentaje_por_evento.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_porcentaje_por_evento)):
        obs = df_val_porcentaje_por_evento.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Types of cybersecuruty events"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_porcentaje_por_evento(val_porcentaje_por_evento):
    """
    Generates data for percentage of events report.

    Parameters
    ----------
    val_porcentaje_por_evento: pandas dataframe
                               Percentage and list of events

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_porcentaje_por_evento = val_porcentaje_por_evento[['Code',
                                                              'Class of event',
                                                              'Events on data sample(%)',
                                                              'List of events']]

    df_val_porcentaje_por_evento.sort_values(['Code'],
                                             ascending=[True],
                                             inplace=True)

    crear_report_porcentaje_por_evento(path_to_output,
                                       TITULO_PORCENTAJE_POR_EVENTO,
                                       df_val_porcentaje_por_evento)



####################################################################################################
def crear_report_fuentes_por_clase(path, titulo, df_val_fuentes_por_clase):
    """
    Generates data source types report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_fuentes_por_clase: pandas dataframe
                              Data source list by type

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Data source list by type:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_fuentes_por_clase.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_fuentes_por_clase)):
        obs = df_val_fuentes_por_clase.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Data source types"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_fuentes_por_clase(val_fuentes_por_clase):
    """
    Generates data for data source types report.

    Parameters
    ----------
    val_fuentes_por_clase: pandas dataframe
                           Data source list by type

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_fuentes_por_clase = val_fuentes_por_clase[['Type',
                                                      'Data sources']]

    df_val_fuentes_por_clase.sort_values(['Type'], ascending=[True], inplace=True)

    crear_report_fuentes_por_clase(path_to_output,
                                   TITULO_FUENTES_POR_CLASE,
                                   df_val_fuentes_por_clase)



####################################################################################################
def generar_plots_fuentes_por_evento(val_fuentes_por_evento):
    """
    Generates a graph with the number of data sources in each event class.

    Parameters
    ----------
    val_fuentes_por_evento: pandas dataframe
                            Dataframe with number of data sources by event class

    Returns
    -------
    None
    """

    etiquetas = val_fuentes_por_evento['Event class'].to_list()
    valores = val_fuentes_por_evento['Number of data sources'].to_list()
    ancho = 0.5

    fig, ax = plt.subplots()

    ax.bar(etiquetas, valores, ancho)

    ax.set_title('Number of data sources by event class')


    plt.savefig(os.path.join(TEMPORAL_PATH, "fuentes_por_evento"), bbox_inches='tight')



####################################################################################################
def crear_report_fuentes_por_evento(path, titulo, df_val_fuentes_por_evento):
    """
    Generates data sources by event class report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_fuentes_por_evento: pandas dataframe
                               Number of datasources by event class

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Number of data sources by event class:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_fuentes_por_evento.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_fuentes_por_evento)):
        obs = df_val_fuentes_por_evento.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "fuentes_por_evento.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Exclusivity"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_fuentes_por_evento(val_fuentes_por_evento):
    """
    Generates data for data sources by event class report.

    Parameters
    ----------
    val_fuentes_por_evento: pandas dataframe
                            Data source list by type

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_fuentes_por_evento = val_fuentes_por_evento[['Event class',
                                                        'Number of data sources']]

    crear_report_fuentes_por_evento(path_to_output,
                                    TITULO_FUENTES_POR_EVENTO,
                                    df_val_fuentes_por_evento)


####################################################################################################
def generar_plots_categorias_por_eventos(val_categorias_evento):
    """
    Generates a graph with the proportion of events of each class in each quality level.

    Parameters
    ----------
    val_categorias_evento : pandas dataframe
                            Quality (low, medium, high) by event class

    Returns
    -------
    None
    """

    etiquetas = ['Quantity', 'Duplicates', 'Completeness', 'Information level', 'Veracity', 'Unknown veracity', 'Relevance']
    ancho = 0.5

    valores_altos = [val_categorias_evento['Quantity high'].mean(),
                     val_categorias_evento['Duplicates high'].mean(),
                     val_categorias_evento['Completeness high'].mean(),
                     val_categorias_evento['Information level high'].mean(),
                     val_categorias_evento['Veracity high'].mean(),
                     val_categorias_evento['Unknown veracity high'].mean(),
                     val_categorias_evento['Relevance high'].mean()]
    valores_aceptables = [val_categorias_evento['Quantity acceptable'].mean(),
                          val_categorias_evento['Duplicates acceptable'].mean(),
                          val_categorias_evento['Completeness acceptable'].mean(),
                          val_categorias_evento['Information level acceptable'].mean(),
                          val_categorias_evento['Veracity acceptable'].mean(),
                          val_categorias_evento['Unknown veracity acceptable'].mean(),
                          val_categorias_evento['Relevance medium'].mean()]
    valores_bajos = [val_categorias_evento['Quantity low'].mean(),
                     val_categorias_evento['Duplicates low'].mean(),
                     val_categorias_evento['Completeness low'].mean(),
                     val_categorias_evento['Information level low'].mean(),
                     val_categorias_evento['Veracity low'].mean(),
                     val_categorias_evento['Unknown veracity low'].mean(),
                     val_categorias_evento['Relevance low'].mean()]

    fig, ax = plt.subplots(figsize=(12, 4))
    fig.suptitle('Proportion of events of each class in each quality level')

    HQ = mpatches.Patch(color='green', label='Good quality level')
    AQ = mpatches.Patch(color='yellow', label='Acceptable quality level')
    LQ = mpatches.Patch(color='red', label='Bad quality level')
    fig.legend(handles=[HQ, AQ, LQ], loc='upper right')

    ax.bar(etiquetas, valores_altos, ancho, label='Good quality level', color='green')
    ax.bar(etiquetas, valores_aceptables, ancho, label='Acceptable quality level', color='yellow', bottom=valores_altos)
    ax.bar(etiquetas, valores_bajos, ancho, label='Bad quality level', color='red', bottom=[valores_altos[j] +valores_aceptables[j] for j in range(len(valores_altos))])


    plt.savefig(os.path.join(TEMPORAL_PATH, "categorias_por_eventos_agrupado"), bbox_inches='tight')



####################################################################################################
def crear_report_categorias_por_evento(path, titulo, df_val_categorias_evento):
    """
    Generates quality by event class report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_categorias_evento: pandas dataframe
                              Quality (low, medium, high) by event class

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Proportion of events of each class in each quality level:</h3>"
    tabla_formateada += "<table align='center' width='100%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_categorias_evento.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_categorias_evento)):
        obs = df_val_categorias_evento.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "categorias_por_eventos_agrupado.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Quality by event class"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_categorias_por_evento(val_categorias_evento):
    """
    Generates data for quality by event class report.

    Parameters
    ----------
    val_categorias_evento: Pandas dataframe
                           Quality (low, medium, high) by event class

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_categorias_evento = val_categorias_evento[['Event class',
                                                      'Quantity high',
                                                      'Quantity acceptable',
                                                      'Quantity low',
                                                      'Duplicates high',
                                                      'Duplicates acceptable',
                                                      'Duplicates low',
                                                      'Duplicates unknown',
                                                      'Completeness high',
                                                      'Completeness acceptable',
                                                      'Completeness low',
                                                      'Information level high',
                                                      'Information level acceptable',
                                                      'Information level low',
                                                      'Veracity high',
                                                      'Veracity acceptable',
                                                      'Veracity low',
                                                      'Unknown veracity high',
                                                      'Unknown veracity acceptable',
                                                      'Unknown veracity low',
                                                      'Relevance high',
                                                      'Relevance medium',
                                                      'Relevance low',
                                                      'Relevance unknown']]

    df_val_categorias_evento.sort_values(['Event class'], ascending=[True], inplace=True)

    crear_report_categorias_por_evento(path_to_output,
                                       TITULO_CATEGORIAS_POR_EVENTO,
                                       df_val_categorias_evento)



####################################################################################################
def generar_plots_eventos_por_tipo(val_eventos_por_tipo):
    """
    Generates a graph with maximum, minimum and average quality for each event type.

    Parameters
    ----------
    val_eventos_por_tipo: pandas dataframe
                          Quality (minimum, average, maximum) by event class

    Returns
    -------
    None
    """

    df_val_eventos_por_tipo_aux = val_eventos_por_tipo[['Event class code',
                                                        'Highest quality',
                                                        'Average quality',
                                                        'Lowest quality']]

    df_val_eventos_por_tipo_aux.set_index('Event class code', inplace=True)

    # etiquetas = ['Highest quality', 'Average quality', 'Lowest quality']
    etiquetas = ['Hi', 'Av', 'Lo']
    colores = ['tab:blue', 'tab:grey', 'tab:orange']
    ancho = 0.5

    try:
        valores_c01 = [df_val_eventos_por_tipo_aux.loc['C01', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C01', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C01', 'Lowest quality']]
    except Exception:
        valores_c01 = [0.0, 0.0, 0.0]

    try:
        valores_c02 = [df_val_eventos_por_tipo_aux.loc['C02', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C02', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C02', 'Lowest quality']]
    except Exception:
        valores_c02 = [0.0, 0.0, 0.0]

    try:
        valores_c03 = [df_val_eventos_por_tipo_aux.loc['C03', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C03', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C03', 'Lowest quality']]
    except Exception:
        valores_c03 = [0.0, 0.0, 0.0]

    try:
        valores_c04 = [df_val_eventos_por_tipo_aux.loc['C04', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C04', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C04', 'Lowest quality']]
    except Exception:
        valores_c04 = [0.0, 0.0, 0.0]

    try:
        valores_c05 = [df_val_eventos_por_tipo_aux.loc['C05', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C05', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C05', 'Lowest quality']]
    except Exception:
        valores_c05 = [0.0, 0.0, 0.0]

    try:
        valores_c06 = [df_val_eventos_por_tipo_aux.loc['C06', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C06', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C06', 'Lowest quality']]
    except Exception:
        valores_c06 = [0.0, 0.0, 0.0]

    try:
        valores_c07 = [df_val_eventos_por_tipo_aux.loc['C07', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C07', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C07', 'Lowest quality']]
    except Exception:
        valores_c07 = [0.0, 0.0, 0.0]

    try:
        valores_c08 = [df_val_eventos_por_tipo_aux.loc['C08', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C08', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C08', 'Lowest quality']]
    except Exception:
        valores_c08 = [0.0, 0.0, 0.0]

    try:
        valores_c09 = [df_val_eventos_por_tipo_aux.loc['C09', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C09', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C09', 'Lowest quality']]
    except Exception:
        valores_c09 = [0.0, 0.0, 0.0]

    try:
        valores_c10 = [df_val_eventos_por_tipo_aux.loc['C10', 'Highest quality'],
                       df_val_eventos_por_tipo_aux.loc['C10', 'Average quality'],
                       df_val_eventos_por_tipo_aux.loc['C10', 'Lowest quality']]
    except Exception:
        valores_c10 = [0.0, 0.0, 0.0]


    fig, (ax01, ax02, ax03, ax04, ax05, ax06, ax07, ax08, ax09, ax10) = plt.subplots(1, 10, figsize=(16, 4), sharey=True)
    fig.suptitle('Maximum, minimum and average quality for each event type')

    HQ = mpatches.Patch(color='tab:blue', label='Highest quality')
    AQ = mpatches.Patch(color='tab:grey', label='Average quality')
    LQ = mpatches.Patch(color='tab:orange', label='Lowest quality')
    fig.legend(handles=[HQ, AQ, LQ], loc='upper right')

    ax01.bar(etiquetas, valores_c01, ancho, color=colores)
    ax01.set_title('C01')

    ax02.bar(etiquetas, valores_c02, ancho, color=colores)
    ax02.set_title('C02')

    ax03.bar(etiquetas, valores_c03, ancho, color=colores)
    ax03.set_title('C03')

    ax04.bar(etiquetas, valores_c04, ancho, color=colores)
    ax04.set_title('C04')

    ax05.bar(etiquetas, valores_c05, ancho, color=colores)
    ax05.set_title('C05')

    ax06.bar(etiquetas, valores_c06, ancho, color=colores)
    ax06.set_title('C06')

    ax07.bar(etiquetas, valores_c07, ancho, color=colores)
    ax07.set_title('C07')

    ax08.bar(etiquetas, valores_c08, ancho, color=colores)
    ax08.set_title('C08')

    ax09.bar(etiquetas, valores_c09, ancho, color=colores)
    ax09.set_title('C09')

    ax10.bar(etiquetas, valores_c10, ancho, color=colores)
    ax10.set_title('C10')


    plt.savefig(os.path.join(TEMPORAL_PATH, "eventos_por_tipo"), bbox_inches='tight')



####################################################################################################
def crear_report_valoracion_eventos_por_tipo(path, titulo, df_val_eventos_por_tipo):
    """
    Generates quality by event type report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_eventos_por_tipo: pandas dataframe
                             Quality (minimum, average, maximum) by event class

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Maximum, minimum and average quality for each event type:</h3>"
    tabla_formateada += "<table align='center' width='80%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_eventos_por_tipo.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_eventos_por_tipo)):
        obs = df_val_eventos_por_tipo.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "eventos_por_tipo.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Quality by event type"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_valoracion_eventos_por_tipo(val_eventos_por_tipo):
    """
    Generates data for quality by event type report.

    Parameters
    ----------
    val_eventos_por_tipo: pandas dataframe
                          Quality (minimum, average, maximum) by event class

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_eventos_por_tipo = val_eventos_por_tipo[['Event class code',
                                                    'Event class',
                                                    'Highest quality',
                                                    'Average quality',
                                                    'Lowest quality']]

    df_val_eventos_por_tipo.sort_values(['Event class code'],
                                        ascending=[True],
                                        inplace=True)

    crear_report_valoracion_eventos_por_tipo(path_to_output,
                                             TITULO_CALIDAD_EVENTOS_POR_TIPO,
                                             df_val_eventos_por_tipo)



####################################################################################################
def generar_plots_fuentes_por_tipo(val_fuentes_por_tipo):
    """
    Generates a graph with maximum, minimum and average quality for each data source type.

    Parameters
    ----------
    val_fuentes_por_tipo : pandas dataframe
                           Data source evaluation structure

    Returns
    -------
    None
    """

    df_val_fuentes_por_tipo_aux = val_fuentes_por_tipo
    df_val_fuentes_por_tipo_aux = val_fuentes_por_tipo.copy(deep=True)

    df_val_fuentes_por_tipo_aux.set_index('Data source type', inplace=True)

    etiquetas = ['Highest quality', 'Average quality', 'Lowest quality']
    colores = ['tab:blue', 'tab:grey', 'tab:orange']
    ancho = 0.5

    valores_own = [df_val_fuentes_por_tipo_aux.loc['Own', 'Highest quality'],
                   df_val_fuentes_por_tipo_aux.loc['Own', 'Average quality'],
                   df_val_fuentes_por_tipo_aux.loc['Own', 'Lowest quality']]
    valores_public = [df_val_fuentes_por_tipo_aux.loc['Public', 'Highest quality'],
                      df_val_fuentes_por_tipo_aux.loc['Public', 'Average quality'],
                      df_val_fuentes_por_tipo_aux.loc['Public', 'Lowest quality']]
    valores_private = [df_val_fuentes_por_tipo_aux.loc['Private', 'Highest quality'],
                       df_val_fuentes_por_tipo_aux.loc['Private', 'Average quality'],
                       df_val_fuentes_por_tipo_aux.loc['Private', 'Lowest quality']]

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(16, 4), sharey=True)
    fig.suptitle('Maximum, minimum and average quality for each data source type')

    ax1.bar(etiquetas, valores_own, ancho, color=colores)
    ax1.set_title('Own data sources')

    ax2.bar(etiquetas, valores_public, ancho, color=colores)
    ax2.set_title('Public data sources')

    ax3.bar(etiquetas, valores_private, ancho, color=colores)
    ax3.set_title('private data sources')


    plt.savefig(os.path.join(TEMPORAL_PATH, "fuentes_por_tipo"), bbox_inches='tight')



####################################################################################################
def crear_report_fuentes_por_tipo(path, titulo, df_val_fuentes_por_tipo):
    """
    Generates quality by data source type report in pdf format.

    Parameters
    ----------
    path: string
          Path to output.
    titulo: string
            Data source name
    df_val_fuentes_por_tipo: pandas dataframe
                             Evaluation structure for each Data source - Event typology

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Maximum, minimum and average quality for each data source type:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_fuentes_por_tipo.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_fuentes_por_tipo)):
        obs = df_val_fuentes_por_tipo.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "fuentes_por_tipo.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Quality by data source type"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_valoracion_fuentes_por_tipo(val_fuentes_por_tipo):
    """
    Generates data for quality by data source type report.

    Parameters
    ----------
    val_fuentes_por_tipo: pandas dataframe
                          Evaluation structure for each Data source - Event typology

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    val_fuentes_por_tipo.sort_values(['Data source type'],
                                     ascending=[True],
                                     inplace=True)

    crear_report_fuentes_por_tipo(path_to_output,
                                  TITULO_CALIDAD_FUENTES_POR_TIPO,
                                  val_fuentes_por_tipo)



####################################################################################################
def generar_plots_fuentes_por_dimension(val_fuentes_por_dimension):
    """
    Generates a graph with data sources assessment, indicating the proportion of records in each
    each quality grade

    Parameters
    ----------
    val_fuentes_por_dimension : pandas dataframe
                                Data source evaluation structure

    Returns
    -------
    None
    """

    df_val_fuentes_por_dimension_aux = val_fuentes_por_dimension.copy(deep=True)
    df_val_fuentes_por_dimension_aux.set_index(['Quality dimension', 'Data source class'], inplace=True)

    etiquetas = ['Own', 'Public', 'Private']
    ancho = 0.5

    colores_green = ['green', 'green', 'green']
    colores_yellow = ['yellow', 'yellow', 'yellow']
    colores_red = ['red', 'red', 'red']

    valores_cantidad_altos = [df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Own'), 'High quality'],
                              df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Public'), 'High quality'],
                              df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Private'), 'High quality']]
    valores_cantidad_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Own'), 'Acceptable quality'],
                                   df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Public'), 'Acceptable quality'],
                                   df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Private'), 'Acceptable quality']]
    valores_cantidad_bajos = [df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Own'), 'Low quality'],
                              df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Public'), 'Low quality'],
                              df_val_fuentes_por_dimension_aux.loc[('Quantity', 'Private'), 'Low quality']]

    valores_duplicados_altos = [df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Own'), 'High quality'],
                                df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Public'), 'High quality'],
                                df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Private'), 'High quality']]
    valores_duplicados_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Own'), 'Acceptable quality'],
                                     df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Public'), 'Acceptable quality'],
                                     df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Private'), 'Acceptable quality']]
    valores_duplicados_bajos = [df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Own'), 'Low quality'],
                                df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Public'), 'Low quality'],
                                df_val_fuentes_por_dimension_aux.loc[('Duplicity', 'Private'), 'Low quality']]

    valores_completitud_altos = [df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Own'), 'High quality'],
                                 df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Public'), 'High quality'],
                                 df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Private'), 'High quality']]
    valores_completitud_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Own'), 'Acceptable quality'],
                                      df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Public'), 'Acceptable quality'],
                                      df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Private'), 'Acceptable quality']]
    valores_completitud_bajos = [df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Own'), 'Low quality'],
                                 df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Public'), 'Low quality'],
                                 df_val_fuentes_por_dimension_aux.loc[('Completeness', 'Private'), 'Low quality']]

    valores_nivel_informacion_altos = [df_val_fuentes_por_dimension_aux.loc[('Information level', 'Own'), 'High quality'],
                                       df_val_fuentes_por_dimension_aux.loc[('Information level', 'Public'), 'High quality'],
                                       df_val_fuentes_por_dimension_aux.loc[('Information level', 'Private'), 'High quality']]
    valores_nivel_informacion_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Information level', 'Own'), 'Acceptable quality'],
                                            df_val_fuentes_por_dimension_aux.loc[('Information level', 'Public'), 'Acceptable quality'],
                                            df_val_fuentes_por_dimension_aux.loc[('Information level', 'Private'), 'Acceptable quality']]
    valores_nivel_informacion_bajos = [df_val_fuentes_por_dimension_aux.loc[('Information level', 'Own'), 'Low quality'],
                                       df_val_fuentes_por_dimension_aux.loc[('Information level', 'Public'), 'Low quality'],
                                       df_val_fuentes_por_dimension_aux.loc[('Information level', 'Private'), 'Low quality']]

    valores_veracidad_altos = [df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Own'), 'High quality'],
                               df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Public'), 'High quality'],
                               df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Private'), 'High quality']]
    valores_veracidad_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Own'), 'Acceptable quality'],
                                    df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Public'), 'Acceptable quality'],
                                    df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Private'), 'Acceptable quality']]
    valores_veracidad_bajos = [df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Own'), 'Low quality'],
                               df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Public'), 'Low quality'],
                               df_val_fuentes_por_dimension_aux.loc[('Veracity', 'Private'), 'Low quality']]

    valores_veracidad_desconocida_altos = [df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Own'), 'High quality'],
                                           df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Public'), 'High quality'],
                                           df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Private'), 'High quality']]
    valores_veracidad_desconocida_aceptables = [df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Own'), 'Acceptable quality'],
                                                df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Public'), 'Acceptable quality'],
                                                df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Private'), 'Acceptable quality']]
    valores_veracidad_desconocida_bajos = [df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Own'), 'Low quality'],
                                           df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Public'), 'Low quality'],
                                           df_val_fuentes_por_dimension_aux.loc[('Unknown veracity', 'Private'), 'Low quality']]



    fig, (ax1, ax2, ax3, ax4, ax5, ax6) = plt.subplots(1, 6, figsize=(16, 4), sharey=True)
    fig.suptitle('Proportion of records in each each quality grade')

    HQ = mpatches.Patch(color='green', label='Good \n quality \n level')
    AQ = mpatches.Patch(color='yellow', label='Acceptable \n quality \n level')
    LQ = mpatches.Patch(color='red', label='Bad \n quality \n level')
    fig.legend(handles=[HQ, AQ, LQ], loc='upper right')

    ax1.bar(etiquetas, valores_cantidad_altos, ancho, color=colores_green)
    ax1.bar(etiquetas, valores_cantidad_aceptables, ancho, color=colores_yellow, bottom=valores_cantidad_altos)
    ax1.bar(etiquetas, valores_cantidad_bajos, ancho, color=colores_red, bottom=[valores_cantidad_altos[j] + valores_cantidad_aceptables[j] for j in range(len(valores_cantidad_altos))])
    ax1.set_title('Quantity')

    ax2.bar(etiquetas, valores_duplicados_altos, ancho, color='green')
    ax2.bar(etiquetas, valores_duplicados_aceptables, ancho, color='yellow', bottom=valores_duplicados_altos)
    ax2.bar(etiquetas, valores_duplicados_bajos, ancho, color='red', bottom=[valores_duplicados_altos[j] + valores_duplicados_aceptables[j] for j in range(len(valores_duplicados_altos))])
    ax2.set_title('Duplicity')

    ax3.bar(etiquetas, valores_completitud_altos, ancho, color='green')
    ax3.bar(etiquetas, valores_completitud_aceptables, ancho, color='yellow', bottom=valores_completitud_altos)
    ax3.bar(etiquetas, valores_completitud_bajos, ancho, color='red', bottom=[valores_completitud_altos[j] + valores_completitud_aceptables[j] for j in range(len(valores_completitud_altos))])
    ax3.set_title('Completeness')

    ax4.bar(etiquetas, valores_nivel_informacion_altos, ancho, color='green')
    ax4.bar(etiquetas, valores_nivel_informacion_aceptables, ancho, color='yellow', bottom=valores_nivel_informacion_altos)
    ax4.bar(etiquetas, valores_nivel_informacion_bajos, ancho, color='red', bottom=[valores_nivel_informacion_altos[j] + valores_nivel_informacion_aceptables[j] for j in range(len(valores_nivel_informacion_altos))])
    ax4.set_title('Information level')

    ax5.bar(etiquetas, valores_veracidad_altos, ancho, color='green')
    ax5.bar(etiquetas, valores_veracidad_aceptables, ancho, color='yellow', bottom=valores_veracidad_altos)
    ax5.bar(etiquetas, valores_veracidad_bajos, ancho, color='red', bottom=[valores_veracidad_altos[j] + valores_veracidad_aceptables[j] for j in range(len(valores_veracidad_altos))])
    ax5.set_title('Veracity')

    ax6.bar(etiquetas, valores_veracidad_desconocida_altos, ancho, color='green')
    ax6.bar(etiquetas, valores_veracidad_desconocida_aceptables, ancho, color='yellow', bottom=valores_veracidad_desconocida_altos)
    ax6.bar(etiquetas, valores_veracidad_desconocida_bajos, ancho, color='red', bottom=[valores_veracidad_desconocida_altos[j] + valores_veracidad_desconocida_aceptables[j] for j in range(len(valores_veracidad_desconocida_altos))])
    ax6.set_title('Unknown veracity')


    plt.savefig(os.path.join(TEMPORAL_PATH, "fuentes_por_dimension"), bbox_inches='tight')



####################################################################################################
def crear_report_fuentes_por_dimension(path, titulo, df_val_fuentes_por_dimension):
    """
    Generates data source quality by dimension report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_fuentes_por_dimension: pandas dataframe
                                  Data source evaluation structure

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Proportion (%) of records in each each quality grade:</h3>"
    tabla_formateada += "<table align='center' width='80%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_fuentes_por_dimension.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_fuentes_por_dimension)):
        obs = df_val_fuentes_por_dimension.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "fuentes_por_dimension.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Data source quality by dimension"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_fuentes_por_dimension(val_fuentes_por_dimension):
    """
    Generates data for data source quality by dimension report.

    Parameters
    ----------
    val_fuentes_por_dimension: pandas dataframe
                               Data source evaluation structure

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_fuentes_por_dimension = val_fuentes_por_dimension[['Quality dimension',
                                                              'Data source class',
                                                              'High quality',
                                                              'Acceptable quality',
                                                              'Low quality']]

    # df_val_fuentes_por_tipo.sort_values(['Data source type'], ascending=[True], inplace=True)

    crear_report_fuentes_por_dimension(path_to_output,
                                       TITULO_CALIDAD_FUENTES_POR_DIMENSION,
                                       df_val_fuentes_por_dimension)



####################################################################################################
def crear_report_eventos_cubiertos_por_fuentes(path, titulo, df_val_eventos_cubiertos_por_fuentes):
    """
    Generates events covered by data sources report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_eventos_cubiertos_por_fuentes: pandas dataframe
                                          Structure with events covered by data source class

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Events covered by each data source class:</h3>"
    tabla_formateada += "<table align='center' width='80%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_eventos_cubiertos_por_fuentes.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_eventos_cubiertos_por_fuentes)):
        obs = df_val_eventos_cubiertos_por_fuentes.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Events covered by data sources"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')

    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



####################################################################################################
def generar_informe_eventos_cubiertos_por_fuentes(val_eventos_cubiertos_por_fuentes):
    """
    Generates data for events covered by data sources report.

    Parameters
    ----------
    val_eventos_cubiertos_por_fuentes: pandas dataframe
                                       Structure with events covered by data source class

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_val_eventos_cubiertos_por_fuentes = val_eventos_cubiertos_por_fuentes[['Event class code',
                                                                              'Event class',
                                                                              'Event typology',
                                                                              'Own data sources',
                                                                              'Public data sources',
                                                                              'Private data sources']]

    df_val_eventos_cubiertos_por_fuentes.sort_values(['Event class code', 'Event typology'],
                                                     ascending=[True, True],
                                                     inplace=True)

    crear_report_eventos_cubiertos_por_fuentes(path_to_output,
                                               TITULO_EVENTOS_CUBIERTOS_POR_FUENTES,
                                               df_val_eventos_cubiertos_por_fuentes)



####################################################################################################
def generar_plots_datos_por_fuente(val_datos_por_fuente):
    """
    Generates a graph with the umber of data records provided by each data source

    Parameters
    ----------
    val_datos_por_fuente: pandas dataframe
                          Structure with number of data records provided by each data source

    Returns
    -------
    None
    """

    val_datos_por_fuente.sort_values(['Data source', 'Cantidad'],
                                     ascending=[True, False],
                                     inplace=True)
    val_datos_por_fuente.rename(columns={'Cantidad': 'Number of data records'},
                                inplace=True)
    val_datos_por_fuente.reset_index(drop=True, inplace=True)

    etiquetas = val_datos_por_fuente['Data source'].to_list()
    valores = val_datos_por_fuente['Number of data records'].to_list()
    ancho = 0.5

    fig, ax = plt.subplots(figsize=(16, 4))

    ax.bar(etiquetas, valores, ancho)

    ax.set_title("Number of data records provided by each data source")


    plt.savefig(os.path.join(TEMPORAL_PATH, "datos_por_fuente"), bbox_inches='tight')



####################################################################################################
def crear_report_datos_por_fuente(path, titulo, df_val_datos_por_fuente):
    """
    Generates data recods by data source report in pdf format.

    Parameters
    ----------
    path: string
          Path to output
    titulo: string
            Report title
    df_val_datos_por_fuente: pandas dataframe
                             Structure with number of data records provided by each data source

    Returns
    -------
    None
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    tabla_formateada = "<br/><h3>Number of data records provided by each data source:</h3>"
    tabla_formateada += "<table align='center' width='80%' border='1' cellspacing='0' cellpadding='2'><tr>"

    for nombre_atributo in df_val_datos_por_fuente.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"

    for ind in range(len(df_val_datos_por_fuente)):
        obs = df_val_datos_por_fuente.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"

    tabla_formateada += "</table>"

    # Insertar grafico
    tabla_formateada += "<br>"
    tabla_formateada += "<center>"
    tabla_formateada += "<img src=" + TEMPORAL_PATH + "datos_por_fuente.png" + ">"
    tabla_formateada += "</center>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        output_file.write(renderizado)

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
        pdf_resultante = os.path.join(path, "Records provided by data source"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')



####################################################################################################
def generar_informe_datos_por_fuente(val_datos_por_fuente):
    """
    Generates data for data recods by data source report.

    Parameters
    ----------
    val_datos_por_fuente: pandas dataframe
                          Structure with number of data records provided by each data source

    Returns
    -------
    None
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    val_datos_por_fuente.sort_values(['Number of data records', 'Data source'],
                                     ascending=[False, True],
                                     inplace=True)
    val_datos_por_fuente.rename(columns={'Number of data records': 'Number of data records provided'},
                                inplace=True)
    val_datos_por_fuente.reset_index(drop=True, inplace=True)

    crear_report_datos_por_fuente(path_to_output,
                                  TITULO_NUMERO_DATOS_POR_FUENTE,
                                  val_datos_por_fuente)



####################################################################################################
def concatenar_pdfs():
    """
    Merges all the necessary pdf files to generate the global report.

    Returns
    -------
    None
    """

    pdfs = []

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Data source ranking.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Data source types.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Records provided by data source.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Quality by data source type.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Data source quality by dimension.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Types of cybersecuruty events.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Events covered by data sources.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Exclusivity.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Quality by event type.pdf')
    pdfs.append(fichero)

    fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Quality by event class.pdf')
    pdfs.append(fichero)


    merger = PdfFileMerger()

    for pdf in pdfs:
        try:
            merger.append(pdf)
        except Exception:
            merger.close()
            print(ERROR_MSG_209, pdf)
            sys.exit()

    try:
        fichero = os.path.join(BASE_PATH, OUTPUT_DIR, 'Global report.pdf')
        merger.write(fichero)
        merger.close()
    except Exception:
        merger.close()
        print(ERROR_MSG_210)
        sys.exit()

    for pdf in pdfs:
        os.remove(pdf)



####################################################################################################
#                                                                                                  #
# Functions for plotting                                                                           #
#                                                                                                  #
####################################################################################################

def makedir(directory_path):
    '''
    Creates a new directory, if it exists, it does nothing.

    Parameters
    ----------
    directory_path: str
        The path of the directory to be created

    Returns
    -------
    None
    '''

    try:
        os.mkdir(directory_path)
    except WindowsError:
        pass



####################################################################################################
def process_valoracion_tipologia(valoracion_tipologia, plot_col):
    '''
    Extracts an argument tuple for matplotlib's bar chart from the evaluation structure.
    Sorts the raw dimension values from highest to lowest, so the first bar will have the highest
    value and so on.
    Also paints the bars with the color corresponding to the level of the dimension.

    Parameters
    ----------
    valoracion_tipologia: pandas.DataFrame
                          Evaluation structure for a single event typology.

    Returns
    -------
    None
    '''

    # Se ordenan los valores de mayor a menor, excepto precio por dato, en el que menor es mejor
    ascending = valoracion_tipologia.columns[1] in ['Precio por dato', 'Veracidad desconocida']
    val = copy.deepcopy(valoracion_tipologia)
    val.sort_values(by=valoracion_tipologia.columns[1], ascending=ascending, inplace=True)

    # Mapeo de los niveles a colores
    dict_map = {GOOD_LEVEL: 'g', ACCEPTABLE_LEVEL: 'y', BAD_LEVEL: 'r'}


    # Los argumentos
    # x: las etiquetas del eje X: las fuentes basicamente
    # height: los valores del eje Y: el valor en bruto para la dimension
    # color: el color de las barras: correspondientes al nivel
    return {'x': list(val[plot_col].values.ravel()),
            'height': list(val[val.columns[1]].values.ravel()),
            'color': list(map(lambda x: dict_map[x], val[val.columns[2]]))
           }



####################################################################################################
def plot_comparison_sources(valoracion_tipologia, tipologia, dimension, save_path, plot_col):
    '''
    Saves a plot of the comparison of a certain data quality dimension between all data sources that
    offer it.

    Parameters
    ----------
    valoracion_tipologia: pandas.DataFrame
        Evaluation structure for a single event typology.
    tipologia: str
        The selected typology
    dimension: str
        The selected data quality dimension
    save_path: str
        Directory in which to save the generated plot

    Returns
    -------
    None
    '''

    plot_kwargs = process_valoracion_tipologia(valoracion_tipologia, plot_col)
    plt.figure()
    plt.xticks(rotation=-45)
    plt.title(u'Valoracion de la dimension %s en % s' % (dimension, tipologia))
    # Otherwise the scale of the Y axis may be such that the bar will be not visible
    plt.ylim(bottom=0)
    index = -(dimension in ['Precio por dato', 'Veracidad desconocida'])
    top = plot_kwargs['height'][index] + plot_kwargs['height'][index] * 0.05
    if not top:
        top = 1
    plt.ylim(top=top)
    plt.xlabel(u"Fuentes")
    plt.ylabel(dimension)
    plt.bar(**plot_kwargs)
    plt.savefig(os.path.join(save_path, dimension), bbox_inches='tight')
    plt.close()



####################################################################################################
####################################################################################################
def generar_plots(valoracion, tipo_plot):
    '''
    Generates comparison plots between data sources for each of the the data quality dimensions.
    Plots are saved in temp folder by default, with one subfolder for each typology and within them,
    a png plot for each quality dimension.

    Parameters
    ----------
    valoracion: pandas.DataFrame
                Evaluation structure for each Data source - Event typology.
    tipo_plot: string
               It indicates the plot type: 'Tipologia' o 'Data source'

    Returns
    -------
    None
    '''

    valoracion = valoracion.copy(deep=True)
    # Sustituye la consistencia por un valor numerico
    valoracion['Consistencia'] = list(map(lambda x: EQUIVALENCIA_CONSISTENCIA_NUMERICA[x], valoracion['Consistencia']))

    # Elimina plots creados en anteriores ejecuciones
    # delete_directory_contents(TEMP_DIR)

    # Tipologias evaluadas
    if tipo_plot == 'Data source':
        subdirectorio = 'Fuentes'
        # columna_seleccionada = 'Tipologia'
    elif tipo_plot == 'Tipologia':
        subdirectorio = 'Tipologias'
        # columna_seleccionada = 'Data source'
    path = os.path.join(TEMP_DIR, subdirectorio)
    makedir(path)

    tipologias = set(valoracion[tipo_plot])
    for tip in tipologias:
        tipologia_path = os.path.join(path, tip)
        # Crea el directorio para plots de esta tipologia
        makedir(tipologia_path)
        for dim in COMPARISON_PLOTS_DIMENSIONS:
            # Extrae fuente, dimension en bruto y su nivel.
            valoracion_tipologia = valoracion.loc[valoracion[tipo_plot] == tip, [tipo_plot, dim, dim + ' nivel']]
            # Crea y guarda el plot
            plot_comparison_sources(valoracion_tipologia, tip, dim, tipologia_path, tipo_plot)
