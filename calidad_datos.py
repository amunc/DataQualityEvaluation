# -*- coding: utf-8 -*-
"""
Created on Mon May 27 09:46:38 2019

@author: Enrique
"""

from time import time
import warnings
import lib_calidad_datos as cd



def main():
    """
    Data quality assessment software
    """

    warnings.filterwarnings("ignore")

####################################################################################################
#                                                                                                  #
# Lectura de las características de la muestra de datos. Carga de ficheros de configuracion y      #
# carga del listado de ficheros de input                                                           #
#                                                                                                  #
####################################################################################################

    # Lectura del separador de datos y periodo de la muestra de datos
    separador, data_period, pc_criteria = cd.leer_caracteristicas_muestra()

    inicio = time()
    print('Work in progress...')

    # Carga de los ficheros de parametrizacion de tipologias de eventos y de
    #   fuentes de datos (data_source.ini y event_typology.ini)
    data_source_parser = cd.cargar_configuracion_fuentes()
    event_typology_parser = cd.cargar_configuracion_tipologias()

    # Carga del listado de ficheros de input
    lista_ficheros_input = cd.cargar_ficheros_input()

    # Crear directorios de salida
    cd.borrar_salida()



####################################################################################################
#                                                                                                  #
# Calculo de las dimensiones de calidad                                                            #
#                                                                                                  #
####################################################################################################


    # Cálculo de las dimensiones de cantidad, completitud, fiabilidad y severidad.
    #   Se realizará fichero a fichero (por chunks) después será neceario agrupar los resultados.
    valoracion = cd.valorar_dimensiones(lista_ficheros_input,
                                        separador,
                                        data_source_parser,
                                        event_typology_parser)

    # Agrupación de todos las valoraciones de los diferentes chunks de datos
    valoracion = cd.compute_valoracion(valoracion)

    # Cálculo del nivel de información (en este punto solo tenemos el número total de campos)
    valoracion = cd.valorar_nivel_informacion(valoracion)

    # Cálculo del precio por dato
    valoracion = cd.valorar_precio_por_dato(valoracion, data_period)

    # Cálculo de duplicados
    valoracion = cd.encontrar_duplicados(valoracion, event_typology_parser)



####################################################################################################
#                                                                                                  #
# Calculo de las dimensiones de calidad normalizadas.                                              #
#                                                                                                  #
####################################################################################################

    # Cáclulo de cantidad normalizada
    valoracion = cd.calcular_cantidad_normalizada(valoracion)

    # Cálculo de dimensiones de calidad normalizadas
    valoracion = cd.calcular_valores_normalizados(valoracion)

    # Cálculo del precio por dato normalizado
    valoracion = cd.calcular_precio_normalizado(valoracion, event_typology_parser)



####################################################################################################
#                                                                                                  #
# Calculo de los niveles de calidad para el informe y valoración de calidad de cada fuente por     #
# tipología.                                                                                       #
#                                                                                                  #
####################################################################################################

    # Cálculo de niveles
    valoracion = cd.calcular_niveles(valoracion, event_typology_parser)

    # Valoracion de calidad las fuentes, por tipologia:
    valoracion = cd.valorar_calidad_tipologia(valoracion)

    # Valoración de la exclusibidad de las fuentes, por tipologías:
    valoracion = cd.valorar_exclusividad(valoracion)

    # Valoración AHP para las tipolgías con mas de una fuente:
    valoracion = cd.generar_matrices_AHP(valoracion, pc_criteria)



####################################################################################################
#                                                                                                  #
# Valoracion total de la calidad                                                                   #
#                                                                                                  #
####################################################################################################

    # Datos para el informe de ranking de fuentes
    valoracion_fuentes = cd.valorar_calidad_global(valoracion)

    # Datos para el porcentaje de eventos y lista de eventos por cada clase de evento
    valoracion_porcentaje_por_evento = cd.valorar_porcentaje_por_evento(valoracion)

    # Datos para el listado de fuentes por cada tipo de fuente:
    valoracion_fuentes_por_clase = cd.valorar_fuentes_por_clase(valoracion)

    # Datos para el número de fuentes por cada clase de evento
    valoracion_fuentes_por_evento = cd.valorar_fuentes_por_evento(valoracion)

    # Datos para el informe de calidad por clase de evento
    valoracion_categorias_evento = cd.valorar_calidad_por_categorias_eventos(valoracion)

    # Datos para el informe de calidad total (max, avg y min) por clase de evento
    valoracion_eventos_por_tipo = cd.valorar_calidad_por_tipo_evento(valoracion)

    # Datos para el informe de calidad total (max, avg y min) por tipo de fuente
    valoracion_fuentes_por_tipo = cd.valorar_calidad_por_tipo_fuente(valoracion_fuentes)

    # Datos para el informe de calidad por tipo de fuente, en cada dimension
    valoracion_fuentes_por_dimension = cd.valorar_calidad_fuente_por_dimension(valoracion)

    # Datos para el informe de eventos cubiertos por tipo de fuente
    valoracion_eventos_cubiertos_por_fuentes = cd.valorar_eventos_cubiertos_por_fuentes(valoracion)

    # Datos para el informe de numero de datos por fuente
    valoracion_datos_por_fuente = cd.valorar_datos_por_fuente(valoracion)



####################################################################################################
#                                                                                                  #
# Generacion de informes                                                                           #
#                                                                                                  #
####################################################################################################

    cd.generar_plots(valoracion, 'Data source')
    cd.generar_informe_fuentes(valoracion, valoracion_fuentes)

    cd.generar_plots(valoracion, 'Tipologia')
    cd.generar_informe_tipologias(valoracion)

    cd.generar_informe_ranking(valoracion_fuentes, valoracion, pc_criteria)

    cd.generar_informe_porcentaje_por_evento(valoracion_porcentaje_por_evento)

    cd.generar_informe_fuentes_por_clase(valoracion_fuentes_por_clase)

    cd.generar_plots_fuentes_por_evento(valoracion_fuentes_por_evento)
    cd.generar_informe_fuentes_por_evento(valoracion_fuentes_por_evento)

    cd.generar_plots_categorias_por_eventos(valoracion_categorias_evento)
    cd.generar_informe_categorias_por_evento(valoracion_categorias_evento)

    cd.generar_plots_eventos_por_tipo(valoracion_eventos_por_tipo)
    cd.generar_informe_valoracion_eventos_por_tipo(valoracion_eventos_por_tipo)

    cd.generar_plots_fuentes_por_tipo(valoracion_fuentes_por_tipo)
    cd.generar_informe_valoracion_fuentes_por_tipo(valoracion_fuentes_por_tipo)

    cd.generar_plots_fuentes_por_dimension(valoracion_fuentes_por_dimension)
    cd.generar_informe_fuentes_por_dimension(valoracion_fuentes_por_dimension)

    cd.generar_informe_eventos_cubiertos_por_fuentes(valoracion_eventos_cubiertos_por_fuentes)

    cd.generar_plots_datos_por_fuente(valoracion_datos_por_fuente)
    cd.generar_informe_datos_por_fuente(valoracion_datos_por_fuente)


    cd.concatenar_pdfs()

    fin = time()
    tiempo = fin - inicio
    print('Total time: ', round(tiempo, 0), 'secs')



####################################################################################################
if __name__ == "__main__":
    main()
