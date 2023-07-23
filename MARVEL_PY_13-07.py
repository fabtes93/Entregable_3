# %%
## INSTALAR LIBRERIA MARVEL
##pip install --upgrade requests
##pip install marvel


# %%
##IMPORTO LOS DATOS DE LOS NOMBRES DE PERSONAJES EN FORMATO JSON

from marvel import Marvel

public_key = "063111bee0d51305278db9a4973a702d"
private_key = "c5f17da3d269b280a3cf8460a2a0b60f4daa2095"

marvel = Marvel(public_key, private_key)

characters = marvel.characters.all()
offset = 0
limit = 99
total_results = 0
characters = []

while total_results < limit:
    response = marvel.characters.all(limit=limit, offset=offset)
    data = response['data']
    results = data['results']
    total_results += len(results)
    characters.extend(results)
    offset += len(results)
    
for character in characters:
    name = character['name']
    comics = [comic['name'] for comic in character['comics']['items']]
    series = [serie['name'] for serie in character['series']['items']]
    description = character['description']
    
    print("Name:", name)
    print("Comics:", comics)
    print("Series:", series)
    print("Description:", description)

##for character in characters['data']['results']:
    ##print(character['name'])

# %%
##!pip install psycopg2-binary
##!pip install --upgrade psycopg2

# %%
import psycopg2
host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port='5439'
database='data-engineer-database'
user='fabianteseyra_coderhouse'
password='ftP4h6VHz8'

conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

cursor = conn.cursor()
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS fabianteseyra_coderhouse.marvel3 (
    name VARCHAR(1000),
    comics VARCHAR(1000),
    series VARCHAR(1000),
    description VARCHAR(1000)
) sortkey(name)
""")
conn.commit()

print("Table created!")

# Eliminar todos los datos existentes en la tabla
cursor.execute("DELETE FROM fabianteseyra_coderhouse.marvel3")
conn.commit()


# %%
##!pip install pandas
import pandas as pd

# %%
data = []
for character in characters:
    name = character['name']
    comics = ', '.join([comic['name'] for comic in character['comics']['items']])
    series = ', '.join([serie['name'] for serie in character['series']['items']])
    description = character['description']
    data.append([name, comics, series, description])

df = pd.DataFrame(data, columns=['name', 'comics', 'series', 'description'])

print(df)

##agrego una columna con la cantidad de apariciones de cada personajes en comics
df['Apariciones_personajes'] = df['comics'].apply(lambda x: len(x.split(', ')))

# %%
cursor.executemany("""
    INSERT INTO fabianteseyra_coderhouse.marvel3 (name, comics, series, description)
    VALUES (%s, %s, %s, %s)
""", data)

conn.commit()




# %%
print(df)


# %%





# %%
print(df)


# %%
#total_apariciones = df['Apariciones_personajes'].sum()
#print(total_apariciones)


# %%
print(df.columns)


# %%
print(df)

# %%

cursor = conn.cursor()

try:
    column_name = 'Apariciones_personajes'
    cursor.execute(f"""
    ALTER TABLE fabianteseyra_coderhouse.marvel3
    ADD COLUMN {column_name} INTEGER
    """)
    conn.commit()
    print(f"La columna se ha agregado correctamente.")
except Exception as e:
    print(f"Ya existe: {str(e)}")
conn.commit()

# %%
cursor = conn.cursor()
for index, row in df.iterrows():
    name = row['name']
    Apariciones_personajes = row['Apariciones_personajes']
    
    # Ejecutar la sentencia SQL UPDATE
    cursor.execute("""
        UPDATE fabianteseyra_coderhouse.marvel3
        SET Apariciones_personajes = %s
        WHERE name = %s
    """, (Apariciones_personajes, name))

conn.commit()



# %%
df['Apariciones_personajes'].unique()

# %%
table_name = 'fabianteseyra_coderhouse.marvel3'

cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")

result = cursor.fetchone()

if result:
    print("La tabla existe en Redshift.")
else:
    print("La tabla no existe en Redshift.")

# %%
cursor.close()
conn.close()