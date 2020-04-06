# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 13:23:26 2019

@author: danimaeztu.com

V: 1.4
Revision: 05/04/2020
"""
# Settable arguments
bq_cost_TB = 5  # In dolars $
bq_cost_MB = bq_cost_TB/1048576


# Functions
def tasa(mon1, mon2):
    """currency exchange rate"""
    from forex_python.converter import CurrencyRates

    c = CurrencyRates()
    return(c.get_rate(mon1, mon2))


def conversor(MB):
    """Use the appropriate unit of data quantity.
    Returns an object with quantity (float) and unit (str)
    """
    class datos:
        def __init__(self, cd, ud):
            self.cd = cd
            self.ud = ud

    GB = round(MB / 1024, 3)
    TB = round(GB / 1024, 3)

    if GB >= 1:
        if TB >= 1:
            cant_datos = TB
            unidad = 'TB'
        else:
            cant_datos = GB
            unidad = 'GB'
    else:
        cant_datos = MB
        unidad = 'MB'

    return(datos(cant_datos, unidad))


def dry(q, proyecto, mon='EUR'):
    """Find out the amount of data and the cost to be moved
    before executing (using the API)
    """
    import math
    from google.cloud import bigquery

    client = bigquery.Client(proyecto)

    job_config = bigquery.QueryJobConfig()
    # We don't want the result, just the estimation of the cost in moved bytes
    job_config.dry_run = True
    # It does not take saved searches (it would output 0)
    job_config.use_query_cache = False

    query_job = client.query(q, job_config=job_config)  # API called

    assert query_job.state == 'DONE'
    assert query_job.dry_run

    MB_pro = round(query_job.total_bytes_processed/(1024**2), 1)

    if MB_pro < 10:
        MB_bill = 10  # BQ always charge 10MB minimum
    else:
        MB_bill = math.ceil(MB_pro)  # BQ always round up

    coste = round(MB_bill * bq_cost_MB * tasa('USD', mon), 4)

    print("Esta query procesará {} {} aproximadamente"
          .format(conversor(MB_pro).cd, conversor(MB_pro).ud))
    print("Con un coste de {}€ aproximados".format(coste))

    return(MB_pro)


def query(q, proyecto, mon='EUR'):
    """Execute the query"""
    from google.cloud import bigquery

    # Project with BigQuery.job.create permissions
    client = bigquery.Client(proyecto)
    # Query execution
    query_job = client.query(q)  # API called
    resultado = query_job.result()  # It waits to finish the job

    df = resultado.to_dataframe()  # Rip to DataFrame

    MB_bill = round(query_job.total_bytes_billed/(1024**2), 1)
    MB_pro = round(query_job.total_bytes_processed/(1024**2), 1)
    coste = round(MB_bill * bq_cost_MB * tasa('USD', mon), 4)

    print("Se han afectado {} filas".format(query_job.num_dml_affected_rows))
    print("Se han procesado {} {}"
          .format(conversor(MB_pro).cd, conversor(MB_pro).ud))
    print("Se han facturado {} {}"
          .format(conversor(MB_bill).cd, conversor(MB_bill).ud))
    print("Con un coste de {}€".format(coste))

    return(df)
