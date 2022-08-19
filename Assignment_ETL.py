# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 13:11:09 2022

@author: Vishwa Saraiya
"""

import pandas as pd

data = pd.read_csv("C:\\Users\\Vishwa Saraiya\\Downloads\\covid-19-state-level-data.csv")
df = pd.DataFrame(data)

df.rename(columns = {'Unnamed: 0':'id'},
            inplace = True)

print(df)

import pyodbc

#Microsoft JDBC Driver 7.4 for SQL Server 7.4.1.0
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=VISHWA;'
                      'Database=temp_assignment;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

cursor.execute('''create table covid_cases(id int IDENTITY(1,1) PRIMARY KEY,case_date date,"state" varchar(max),fips int,cases int,deaths int) ''')

print('Table created successfully')


for row in df.itertuples():
    cursor.execute('''
                INSERT INTO covid_cases (case_date, state,fips,cases,deaths)
                VALUES (?,?,?,?,?)
                ''',
                row.date,
                row.state,
                row.fips,
                row.cases,
                row.deaths
                )
    
print('Data Inserted in table successfuly')

conn.commit()
cursor.close()
conn.close()