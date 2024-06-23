FROM quay.io/astronomer/astro-runtime:11.3.0

USER root
COPY ./dbt_project ./dbt_project
COPY --chown=astro:0 . .

USER astro
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt && \
    source dbt_project/dbt.env && \
    deactivate

WORKDIR $AIRFLOW_HOME

USER root
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc

#Debian 11
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
# optional: for unixODBC development headers
RUN apt-get install -y unixodbc-dev
RUN apt-get update -qq && apt-get install -y unixodbc

USER astro
