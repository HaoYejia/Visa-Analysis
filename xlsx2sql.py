import sqlalchemy
import sqlite3
import pandas
import re
import os
import time


workPath = "E:\\OneDrive - Rose-Hulman Institute of Technology\\Rose-Hulman\\Career\\H1bData\\H1bAnalysis\\"
inputXlsxPath = "GovData\\"
#inputXlsxName = "LCA_Disclosure_Data_FY2020_Q1.xlsx"

outputDbPath = "Python Analysis 2\\"
outputDbName = "lca_raw_data.db"



dbEngine = sqlalchemy.create_engine("sqlite:///" + workPath + outputDbPath + outputDbName)
#while dbEngine.begin() as connection:

with sqlite3.connect(workPath + outputDbPath + outputDbName) as conn: 
#with dbEngine.connect() as conn: 
        for inputXlsxName in os.listdir(workPath + inputXlsxPath): 
                if re.match(r"^LCA.*\.xlsx$", inputXlsxName):
                        df = pandas.read_excel(workPath + inputXlsxPath + inputXlsxName)
                        df = df.astype(str)

                        try:
                                df.to_sql(name = inputXlsxName.lower().removesuffix(".xlsx"), con = conn, if_exists = 'replace',index = False)
                        except Exception as e:
                                print(e)
                        print(inputXlsxName)
                        print(df)
                        print("=======================")
                        time.sleep(10)