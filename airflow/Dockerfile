FROM astrocrpublic.azurecr.io/runtime:3.3-9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
USER astro

RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.5" "dbt-bigquery==1.12.1"