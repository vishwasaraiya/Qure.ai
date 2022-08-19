# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 01:33:59 2022

@author: Vishwa Saraiya
"""
import slack
import pyodbc
import schedule
import time
import pandas.io.sql as sqlio

def func():
#Microsoft JDBC Driver 7.4 for SQL Server 7.4.1.0
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=VISHWA;'
                          'Database=temp_assignment;'
                          'Trusted_Connection=yes;')
    
    x = [3,4,5,6]
    
    for i in x:
        sql = f"""SELECT
        	top 3
        	datename(month,cc.case_date) as month_name,
        	cc.state,
        	SUM(cc.deaths) state_death,
        	(select SUM(cs.deaths) from covid_cases cs where MONTH(cs.case_date) = 3) as total_deaths,
        	(SUM(cc.deaths)*100)/(select SUM(cs.deaths) from covid_cases cs where MONTH(cs.case_date) = 3) as percent_of_total
        FROM
        	covid_cases as cc
        where 
        	MONTH(cc.case_date) = {i}
        GROUP by 
        	datename(month,cc.case_date),
        	cc.state
        order by 
        	SUM(cc.deaths) DESC"""
            
        data = sqlio.read_sql_query(sql, conn)
        print(data)
        
        month_msg = text = 'Month - ' + str(data['month_name'][0])
    #    SLACK_TOKEN="xoxb-3973537363057-3957990396789-GOwSX4uBh73wOZWF8Fih6gyT"
    #    client = slack.WebClient(token=SLACK_TOKEN)
    #    client.chat_postMessage(channel='#general',text=month_msg)
        
        new_text = ''
        for j in range(0, len(data)):
            text = '\n' + str(data['state'][j]) + ' - ' + str(data['state_death'][j]) + ', ' + str(data['percent_of_total'][j])+'%'
            new_text = new_text + text
        
        SLACK_TOKEN="xoxb-3973537363057-3957990396789-GOwSX4uBh73wOZWF8Fih6gyT"
         
        client = slack.WebClient(token=SLACK_TOKEN)
         
        client.chat_postMessage(channel='#general',text=month_msg+new_text)
                 
    
    conn.close()
        
schedule.every(1).minute.do(func)

while True:
    schedule.run_pending()
    time.sleep(1)
                            
