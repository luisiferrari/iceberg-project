FROM python:3.10-slim

WORKDIR /usr/app

RUN pip install --no-cache-dir dbt-core dbt-trino

CMD ["tail", "-f", "/dev/null"]