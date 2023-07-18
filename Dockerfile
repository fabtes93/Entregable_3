

FROM apache/airflow:2.6.2

RUN python -m pip install --upgrade pip
RUN python -m pip install marvel
RUN python -m pip install pandas
RUN python -m pip install psycopg2-binary

USER root

USER airflow

COPY ./requirements.txt /

#RUN pip install -r requirements.txt
# RUN python -m pip install apache-airflow-providers-apache-spark
# RUN python -m pip install 'apache-airflow[amazon]'
# RUN python -m pip install apache-airflow-providers-amazon

# docker build -t apache/airflow:2.3.3_marvel4 .