FROM apache/airflow:2.5.0
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
# RUN pip install --user snowflake-connector-python snowflake-sqlalchemy apache-airflow-providers-snowflake
RUN pip install --no-cache-dir --user -r /requirements.txt