# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import pandas as pd

import mysql.connector


conn = mysql.connector.connect(host='localhost',user='root', password='febrero66', database='bitso_api')

c= conn.cursor()

reponse = requests.get("https://api.bitso.com/v3/trades/?book=btc_mxn")
response_json = reponse.json()

datos =response_json['payload']

df = pd.DataFrame( datos )

intial_query = """ INSERT INTO trades_bitso_v 
(created_at,book,amount,maker_side,price,tid)
VALUES
"""
values_q = ",".join(["""('{}','{}','{}','{}','{}','{}')""".format(
    row.book,
    row.created_at,
    row.amount,
    row.maker_side,
    row.price,
    row.tid) for idx, row in df.iterrows() ])

end_q = """ ON DUPLICATE KEY UPDATE
        book= values(book),
        created_at = values(created_at),
        amount = values(amount),
        maker_side = values(maker_side),
        price = values(price),
        tid = values(tid);"""
        
query = intial_query + values_q + end_q

print(query)

row = c.execute(query)

print(row)

querySelect = """" SELECT * FROM 'trades_bitso_v' """

rowsSelect = c.execute(querySelect)

print( rowsSelect)
