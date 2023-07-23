from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import pandas as pd
import marvel

public_key = "063111bee0d51305278db9a4973a702d"
private_key = "c5f17da3d269b280a3cf8460a2a0b60f4daa2095"

redshift_conn_id = 'redshift_default'
table_name = 'fabianteseyra_coderhouse.marvel'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 9),
}

dag = DAG('marvel_dag', default_args=default_args, schedule_interval=None)

def buscarinfomarvel():
    marvel_api = marvel.Marvel(public_key, private_key)
    characters = marvel_api.characters.all()
    offset = 0
    limit = 99
    total_results = 0
    characters_data = []

    while total_results < limit:
        response = marvel_api.characters.all(limit=limit, offset=offset)
        data = response['data']
        results = data['results']
        total_results += len(results)
        characters_data.extend(results)
        offset += len(results)

    return characters_data

def personajesmarvel():
    characters_data = buscarinfomarvel()

    data = []
    for character in characters_data:
        name = character['name']
        comics = ', '.join([comic['name'] for comic in character['comics']['items']])
        series = ', '.join([serie['name'] for serie in character['series']['items']])
        description = character['description']
        data.append([name, comics, series, description])

    df = pd.DataFrame(data, columns=['name', 'comics', 'series', 'description'])
    df['Apariciones_personajes'] = df['comics'].apply(lambda x: len(x.split(', ')))

    return df

def guardar_csv(**context):
    ti = context['task_instance']
    df = ti.xcom_pull(task_ids='personajesmarvel')
    csv_filename = '/tmp/marvel_data.csv'
    df.to_csv(csv_filename, index=False)
    return csv_filename

def cargar_datos_redshift(**context):
    csv_filename = context['task_instance'].xcom_pull(task_ids='guardar_csv')
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        result = cursor.fetchone()
        if result:
            print("La tabla existe en Redshift.")
        else:
            print("La tabla no existe en Redshift.")
            cursor.execute(f"CREATE TABLE {table_name} (name VARCHAR(255), comics VARCHAR(255), series VARCHAR(255), description TEXT, Apariciones_personajes INTEGER)")
            conn.commit()
            print("Tabla creada en Redshift.")

        # Cargar datos desde el CSV a la tabla en Redshift
        with open(csv_filename, 'r') as f:
            cursor.copy_from(f, table_name, sep=',', columns=('name', 'comics', 'series', 'description', 'Apariciones_personajes'))

        conn.commit()
        print("Datos cargados en Redshift.")
    except Exception as e:
        print(f"Error al verificar la tabla: {str(e)}")
    finally:
        cursor.close()
        conn.close()

with dag:
    task_buscarinfomarvel = PythonOperator(
        task_id='buscarinfomarvel',
        python_callable=buscarinfomarvel
    )

    task_personajesmarvel = PythonOperator(
        task_id='personajesmarvel',
        python_callable=personajesmarvel
    )

    task_guardar_csv = PythonOperator(
        task_id='guardar_csv',
        python_callable=guardar_csv,
        provide_context=True  
    )

    task_tablamarvel = PythonOperator(
        task_id='tablamarvel',
        python_callable=cargar_datos_redshift,
        provide_context=True  
    )

    task_buscarinfomarvel >> task_personajesmarvel >> task_guardar_csv >> task_tablamarvel