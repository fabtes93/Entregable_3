
# Utilizar una imagen de Python como base
FROM python:3.8.10

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

# Instalar la librería "marvel" utilizando pip
RUN pip install marvel

COPY . .

CMD [ "python", "MARVEL_PY.py" ]
