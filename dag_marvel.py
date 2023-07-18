from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import pandas as pd
import marvel


public_key = "063111bee0d51305278db9a4973a702d"
private_key = "c5f17da3d269b280a3cf8460a2a0b60f4daa2095"

host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
database = 'data-engineer-database'
user = 'fabianteseyra_coderhouse'
password = 'ftP4h6VHz8'
table_name = 'fabianteseyra_coderhouse.marvel'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 9),
}

dag = DAG('marvel_dag', default_args=default_args, schedule_interval=None)

def buscarinfomarvel():
    marvel = Marvel(public_key, private_key)
    characters = marvel.characters.all()
    offset = 0
    limit = 99
    total_results = 0
    characters_data = []

    while total_results < limit:
        response = marvel.characters.all(limit=limit, offset=offset)
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


def tablamarvel():
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

    task_tablamarvel = PythonOperator(
        task_id='tablamarvel',
        python_callable=tablamarvel
    )

    task_buscarinfomarvel >> task_personajesmarvel >> task_tablamarvel