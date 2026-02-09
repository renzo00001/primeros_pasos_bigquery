# Revisar si tenemos instalado la biblioteca de bigquery -> !pip show google-cloud-bigquery
# instalando bigquery  ->  pip show google-cloud-bigquery
import pandas as pd
import numpy as np
import os
from google.cloud import bigquery
import re

credenciales_bigquery= r'C:\ruta\de_tu\archivo\crecenciales_bigquery.json'

def Realizar_Consulta(sql_query):
    # iniciar sesion con las credicianles 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
    cliente = bigquery.Client()
    ejecutar_query = cliente.query(sql_query)
    resultado = [dict(fila) for fila in ejecutar_query.result()]

    df = pd.DataFrame(resultado)
    return df

def Enviar_datos_por_lote(df,nombre_tabla,fecha_archivo):
    """Sube un DataFrame completo a BigQuery optimizando el esquema."""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
    cliente = bigquery.Client()
    
    # Configuración del motor de carga
    job_config = bigquery.LoadJobConfig(
        # Especificamos que si la tabla existe, agregue los datos (append)
        # O usa 'WRITE_TRUNCATE' para sobrescribir toda la tabla
        write_disposition="WRITE_APPEND",
        # Detecta automáticamente el esquema (DATE, STRING, FLOAT, etc.)
        autodetect=True,
    )

    try:
        # Carga por lote usando el motor de PyArrow (muy rápido)
        job = cliente.load_table_from_dataframe(df, nombre_tabla, job_config=job_config)
        job.result()  # Espera a que se complete la carga
        print(f"Éxito: Se subieron {len(df)} filas a {nombre_tabla} , Archivo = {fecha_archivo}")
    except Exception as e:
        print(f"Fecha_Archivo: {fecha_archivo} Error en la carga: {e} ")



def Transformar_datos(df_Data,fecha_archivo):
    try:
        # Modificar el Formato de Fechas para la base de datos 
        df_Data['Fecha'] = pd.to_datetime(df_Data['Fecha'],unit='D',origin='1899-12-30',errors='coerce')
        df_Data['Fecha_HU'] = pd.to_datetime(df_Data['Fecha_HU'],unit='D',origin='1899-12-30',errors='coerce')
        df_Data['CreadoEl'] = pd.to_datetime(df_Data['CreadoEl'] , unit='D' , origin='1899-12-30',errors='coerce')
        df_Data['FECHA VENCIMIENTO']= pd.to_datetime(df_Data['FECHA VENCIMIENTO'] , unit='D' , origin='1899-12-30',errors='coerce')

        # Cambiamos los nombres de columnas 
        mapeo_columnas = {
        # Fechas y Tiempos
        'Fecha': 'Fecha_Reporte',
        'FECHA VENCIMIENTO': 'Fecha_Vencimiento',
        'Dias': 'DiasDePermanencia',
        'Dias Redireccion': 'DiasRedireccion',
        
        # Datos de Producto/Proveedor
        'CodGA': 'CodigoArticulo',
        'GrAr': 'NombreArticulo',
        'DescMaterial': 'NombreMaterial',
        'PUnit': 'PrecioUnidad',
        'CodProveedor': 'RUC_Proveedor',
        
        # Operaciones y Stock
        'Stock Tda Unidades': 'Stock_Tda_Unidades',
        'Cob Tda': 'Cob_Tda',
        'cob <=5': 'cob_menor_o_igual_a_5',
        'Tipo tda': 'TipoTienda',
        'Ce.Origen': 'CentroOrigen',
        'Ce': 'CentroDistribucion',
        
        # Gestión de Paletas y Otros
        'Status HU': 'StatusHU',
        'Gerencia Paleta': 'GerenciaPaleta',
        'Dueño Paleta': 'DunioPaleta',
        'VentaPromedio 30D': 'VentaPromedio30D',
        'Grp Abast.': 'Grupo_Abastecimiento',
        'Alerta Paleta': 'AlertaPaleta'}
        df_Data.rename(columns=mapeo_columnas,inplace=True)
        df_Data['Envio'] = 3 

        # Modificamos el formato 
        df_Data['Material'] = df_Data['Material'].astype(str)
        codigos_articulo = df_Data['CodigoArticulo'].astype(str)
        df_Data['GrComp'] = np.where(
                codigos_articulo.str.len() < 8,
                codigos_articulo.str[:1],
                codigos_articulo.str[:2]
            ).astype(int)
        # Modificando columnas al formato admitido de BIGquery
        columnas_string = ['CentroDistribucion', 'Almacen', 'StatusHU', 'Hora', 'Gestor']
        for col in columnas_string:
            df_Data[col] = df_Data[col].astype(str).replace(['nan', 'None', 'NaT'], None)
        
        df_Data.drop(columns=['Contar'],inplace=True)



        return (True,0)
    except Exception as e :
        return (False,f'ocurrio un error: {e}  , en la fecha {fecha_archivo}')



# Orquestador de las funciones 

Tabla_bigquery = 'cambialo.con.el.ID.de.tu.tabla.bigquery'
ruta_carpeta = r'C:\ruta\de\tu\carpeta\archivos'

for archivo in os.listdir(ruta_carpeta):
    if archivo.strip().lower()[0:12] =='priorización':
        extraer_fecha = re.search(r'(\d{2}\.\d{2}\.\d{4})',archivo).group(1)
        fecha_nueva= pd.to_datetime(extraer_fecha,dayfirst=True).strftime('%Y-%m-%d')

        # comprobar si la fecha del archivo existe en la tabla
        sql_query = f""" SELECT COUNT(*) AS conteo_filas  FROM  {Tabla_bigquery} WHERE Fecha_Reporte = '{fecha_nueva}'; """
        df_bigquery = Realizar_Consulta(sql_query)

        # en caso existan datos , saltara al siguiente archivo
        if df_bigquery['conteo_filas'][0]>0 : continue
        
        ruta_archivo = os.path.join(ruta_carpeta,archivo)
        df_data =  pd.read_excel(ruta_archivo,sheet_name='DATA',engine='pyxlsb')

        # enviamos el dataframe para transformar y preparar los datos antes de enviar a bigquery
        respuesta = Transformar_datos(df_data,fecha_nueva)
        # Control de error , en caso algo fallo 
        if not respuesta[0]: 
            print(respuesta)
            continue 
        
        # Enviamos los datos a bigquery , ya limpios
        Enviar_datos_por_lote(df_data,Tabla_bigquery,fecha_nueva)
        
    
    
