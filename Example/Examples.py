# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 17:18:51 2020

@author: danimaeztu.com
"""
import os
import sys
import google.auth

# Path of the cloned repo
path = ""  # TODO: Insert the path of the cloned repo
os.chdir(path)

# Import Big Query Cost Functions
sys.path.append(os.getcwd()+"\\Source")
import bqCost as bqc

# Google Cloud authentication
credentials, project = google.auth.default()  # Allways print a warning
proyecto = ""  # TODO: Insert a Google Cloud project with Big Query Billing permissions.

# SQL QUERY (open data example)
QUERY = """
SELECT operator, count(operator) as cantidad
FROM `bigquery-public-data.catalonian_mobile_coverage.mobile_data_2015_2017`
GROUP BY operator
ORDER BY cantidad DESC
"""

# Find out the amount of data and the cost to be moved before executing
bqc.dry(QUERY, proyecto)

# Query execution
df = bqc.query(QUERY, proyecto)

print(df.head(10))
