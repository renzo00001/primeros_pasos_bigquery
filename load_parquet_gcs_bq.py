# SIRVE PARA CARGAR DATOS DESDE Google Cloud Storage hacia Bigquery 
from google.cloud import bigquery
def cargar_parquet_desde_gcs_a_tabla_existente():
    # credenciales_bigquery : es una variable que guarda las credenciales de tu tabla bigquery 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
    client = bigquery.Client()

    # ID de tu tabla actual
    table_id = 'ID_Tabla'
    
    # URI de los archivos en GCS (puedes usar asterisco para varios archivos)
    uri = "gs://cambialo.por.tu.ruta.gs/archivo.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # 'WRITE_APPEND' asegura que los datos se agreguen al final de la tabla
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Si el Parquet tiene columnas nuevas que quieres agregar a la tabla, 
        # activa esta opción:
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )

    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        
        load_job.result()  # Espera a que la carga termine
        
        # Verificación
        destination_table = client.get_table(table_id)
        print(f"Carga completada. Ahora la tabla tiene {destination_table.num_rows} filas.")
        
    except Exception as e:
        print(f"Error al cargar Parquet a BigQuery: {e}")

# Ejecutar la función
cargar_parquet_desde_gcs_a_tabla_existente()
