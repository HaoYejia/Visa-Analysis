import os
import re
import math
import numpy
import pathlib
import pandas
import sqlalchemy

class AnalyzeQuarterLCA():
    def __init__(self, inputTableName, majorName, majorSOC, VisaType, VisaStatus, DBPath):
        '''
        self.majorName = "ECE"
        self.majorSOC = ["17-2070", "17-2071", "17-2071.00", "17-2072", "17-2072.00",
                         "17-2073", "17-2073.00", "17-2074", "17-2074.00", "17-2076", "17-2076.00"]

        self.analyzedVisaType = "H-1B"
        self.analyzedVisaStatus = "Certified"

        self.DBworkPath = "C:\\Users\\haoy2\\OneDrive - Rose-Hulman Institute of Technology\\Rose-Hulman\\Career\\H1bData\\H1bAnalysis\\Python Analysis\\"
        '''

        self.majorName = majorName
        self.majorSOC = majorSOC

        self.analyzedVisaType = VisaType
        self.analyzedVisaStatus = VisaStatus

        self.DBworkPath = DBPath

        self.inputRawDataDBConfig = {
            'user': 'root',
            'password': '8LtM1zFleE9wIlMJ1F5M',
            'host': '10.47.240.3',
            'database': 'lca_raw_data',
        }

        self.inputTableName = inputTableName  # "lca_disclosure_data_fy2020_q1"

        self.outputEmployerDataDBConfig = {
            'database': 'lca_quarterly_analyzed_employer_data',
        }

        self.outputLocationDataDBConfig = {
            'database': 'lca_quarterly_analyzed_location_data',
        }

        self.outputEmployerDataTableName = (
                    self.analyzedVisaType + "_" + self.analyzedVisaStatus + "_" + "analyzed_result" + "_" + self.inputTableName).lower().replace(
            "-", "_")
        self.outputLocationDataTableName = (
                    "worksite_location_analyzed_result" + "_" + self.inputTableName).lower().replace("-", "_")

        # The essential columns useful for analysis
        self.essentialCol = ["CASE_NUMBER",
                             "CASE_STATUS",
                             "RECEIVED_DATE",
                             "DECISION_DATE",
                             "VISA_CLASS",
                             "JOB_TITLE",
                             "SOC_CODE",
                             "SOC_TITLE",
                             "FULL_TIME_POSITION",
                             "BEGIN_DATE",
                             "NEW_EMPLOYMENT",
                             "CONTINUED_EMPLOYMENT",
                             "CHANGE_PREVIOUS_EMPLOYMENT",
                             "NEW_CONCURRENT_EMPLOYMENT",
                             "CHANGE_EMPLOYER",
                             "AMENDED_PETITION",
                             "EMPLOYER_NAME",
                             "EMPLOYER_CITY",
                             "EMPLOYER_STATE",
                             "EMPLOYER_POSTAL_CODE",
                             "EMPLOYER_COUNTRY",
                             "EMPLOYER_PROVINCE",
                             "NAICS_CODE",
                             "WAGE_RATE_OF_PAY_FROM",
                             "WAGE_RATE_OF_PAY_TO",
                             "WAGE_UNIT_OF_PAY",
                             "PREVAILING_WAGE",
                             "PW_UNIT_OF_PAY"]

        # Mapping table for redundant employer names
        self.identicalEmployerName = {
            "ABB" : "ABB",
            "ABB ENTERPRISE SOFTWARE" : "ABB",
            "ABB ENTERPRISE SOFTWARE  (AN ABB COMPANY)" : "ABB",
            "ALSTOM SIGNALING" : "ALSTOM",
            "ALSTOM GRID" : "ALSTOM",
            "ALSTOM TRANSPORTATION" : "ALSTOM",
            "AMAZON DATA SERVICES" : "AMAZON",
            "AMAZONCOM SERVICES" : "AMAZON",
            "ATRONIX ACQUISITION CORP" : "ATRONIX ACQUISITION CORP",
            "ATRONIX ACQUISITION CORPORATION" : "ATRONIX ACQUISITION CORP",
            "BORGWARNER PDS (ANDERSON)" : "BORGWARNER",
            "BORGWARNER PDS (USA)" : "BORGWARNER",
            "BORGWARNER TECHNOLOGIES SERVICES" : "BORGWARNER",
            "BURNS & MCDONNELL ENGINEERING COMPANY" : "BURNS & MCDONNELL",
            "BURNS & MCDONNELL WESTERN ENTERPRISES" : "BURNS & MCDONNELL",
            "CANOO TECHNOLOGIES" : "CANOO",
            "CANOO" : "CANOO",
            "CIRRUS LOGIC INTERNATIONAL SEMICONDUCTOR LTD" : "CIRRUS LOGIC",
            "CUMMINS EMISSION SOLUTIONS" : "CUMMINS",
            "DEERE AND COMPANY" : "DEERE & COMPANY",
            "DISH WIRELESS LLC" : "DISH WIRELESS",
            "FACEBOOK" : "META",
            "GE ENERGY MANAGEMENT SERVICES" : "GENERAL ELECTRIC",
            "GE GRID SOLUTIONS" : "GENERAL ELECTRIC",
            "GE PRECISION HEALTHCARE" : "GENERAL ELECTRIC",
            "GE RENEWABLES GRID" : "GENERAL ELECTRIC",
            "GENERAL ELECTRIC COMPANY" : "GENERAL ELECTRIC",
            "GENERAL ELECTRIC COMPANY (GE GLOBAL RESEARCH CENTER)" : "GENERAL ELECTRIC",
            "HCL AMERICA SOLUTIONS": "HCL AMERICA",
            "INFINEON TECHNOLOGIES AMERICAS CORP" : "INFINEON TECHNOLOGIES",
            "INTEL AMERICAS" : "INTEL",
            "INTEL CORPORATION" : "INTEL",
            "INTEL FEDERAL" : "INTEL",
            "INTEL MASSACHUSETTS" : "INTEL",
            "INTEL NDTM US" : "INTEL",
            "L&T TECHNOLOGY SERVICES LIMITED" : "L&T TECHNOLOGY SERVICES",
            "MICRON TECHNOLOGY UTAH" : "MICRON TECHNOLOGY",
            "MOTOROLA SOLUTIONS" : "MOTOROLA",
            "MOTOROLA MOBILITY" : "MOTOROLA",
            "MOTT MACDONALD GROUP" : "MOTT MACDONALD",
            "QUALCOMM ATHEROS" : "QUALCOMM",
            "QUALCOMM INNOVATION CENTER" : "QUALCOMM",
            "QUALCOMM ORPORATED" : "QUALCOMM",
            "QUALCOMM TECHNOLOGIES" : "QUALCOMM",
            "RENESAS DESIGN NORTH AMERICA" : "RENESAS ELECTRONICS",
            "RENESAS ELECTRONICS AMERICA" : "RENESAS ELECTRONICS",
            "RIVIAN AUTOMOTIVE" : "RIVIAN",
            "SAMSUNG SEMICONDUCTOR" : "SAMSUNG",
            "SAMSUNG AUSTIN SEMICONDUCTOR" : "SAMSUNG",
            "SAMSUNG AUSTIN SEMICONDUCTOR LLC" : "SAMSUNG",
            "SAMSUNG ELECTRONICS AMERICA" : "SAMSUNG",
            "SAMSUNG RESEARCH AMERICA" : "SAMSUNG",
            "SAMSUNG SEMICONDUCTOR" : "SAMSUNG",
            "SCHNEIDER ELECTRIC ENGINEERING SERVICES" : "SCHNEIDER ELECTRIC",
            "SCHNEIDER ELECTRIC IT CORPORATION" : "SCHNEIDER ELECTRIC",
            "SCHNEIDER ELECTRIC USA" : "SCHNEIDER ELECTRIC",
            "SIEMENS ENERGY" : "SIEMENS",
            "SIEMENS INDUSTRY" : "SIEMENS",
            "SIEMENS INDUSTRY SOFTWARE" : "SIEMENS",
            "SIEMENS MEDICAL SOLUTIONS USA" : "SIEMENS",
            "SIEMENS MOBILITY" : "SIEMENS",
            "SK HYNIX MEMORY SOLUTIONS AMERICA" : "SK HYNIX",
            "SK HYNIX NAND PRODUCT SOLUTIONS CORP" : "SK HYNIX",
            "TEK LABS" : "TEKLABS",
            "ZF PASSIVE SAFETY SYSTEMS US" : "ZF",
            "ZF NORTH AMERICA" : "ZF"
        }

        

        self.inputDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.inputRawDataDBConfig['database'] + ".db")

        self.outputEmployerDataDBEngine = sqlalchemy.create_engine(
           "sqlite:///" + self.DBworkPath + self.outputEmployerDataDBConfig[
                'database'] + ".db")

        self.outputLocationDataDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.outputLocationDataDBConfig[
                'database'] + ".db")
        
        # Raw, unprocessed dataframe
        self.rawDataFrame = pandas.read_sql("SELECT * FROM " + self.inputTableName, self.inputDBEngine)

        self.cleanedDataFrame = pandas.DataFrame()


    def preprosessData(self):
        try:
            self.inputDBEngine.connect().execute(sqlalchemy.text("DROP TABLE " + self.inputTableName + "; "))
        except:
            pass

        self.inputDBEngine.connect().execute(sqlalchemy.text(self.createQueryString()))

    def createQueryString(self):

        queryString = ""

        tableNames = sqlalchemy.inspect(self.inputDBEngine).get_table_names()
        lcaTables = [table for table in tableNames if table.startswith("lca_disclosure_data")]

        # queryString += "DROP TABLE " + self.inputTableName + "; "
        queryString += "CREATE TABLE " + self.inputTableName + " AS "

        for table in lcaTables:
            if table == tableNames[-1]:
                queryString += "SELECT * FROM " + self.inputRawDataDBConfig['database'] + "." + table + "; "
            else:
                queryString += "SELECT * FROM " + self.inputRawDataDBConfig['database'] + "." + table + " UNION "

        print(queryString)

        return queryString
    
    def cleanEmployerName(self, name : str):
        result = name
        # Merge similar employer names to same name by removing ",", ",", "INC", "LLC" and put every name in upper case
        result = result.upper()
        result = re.sub('[.,]|INC|LLC|LLP|LP|^\s+|\s+$|\s+(?=\s)', "", result)
        result = result.rstrip()

        if result in self.identicalEmployerName:
            result = self.identicalEmployerName[result]

        return result

    def cleanData(self):
        # Extract only the essential columns
        self.cleanedDataFrame = self.rawDataFrame[self.essentialCol]

        pass

        # Convert all text date to datetime format
        self.cleanedDataFrame["RECEIVED_DATE"] = pandas.to_datetime(self.cleanedDataFrame["RECEIVED_DATE"],
                                                                    errors='coerce')
        self.cleanedDataFrame["DECISION_DATE"] = pandas.to_datetime(self.cleanedDataFrame["DECISION_DATE"],
                                                                    errors='coerce')
        self.cleanedDataFrame["BEGIN_DATE"] = pandas.to_datetime(self.cleanedDataFrame["BEGIN_DATE"], errors='coerce')

        # Convert all postal code to 5-digit postal code
        self.cleanedDataFrame["EMPLOYER_POSTAL_CODE"] = self.cleanedDataFrame["EMPLOYER_POSTAL_CODE"].str.extract(
            r'(\d{5})')

        # Add year to the dataframe
        self.cleanedDataFrame.insert(loc=1, column="YEAR",
                                     value=self.cleanedDataFrame[
                                         "DECISION_DATE"].dt.year)  # Add year of job to dataframe

        # Delete all dollar signs and commas of wages
        self.cleanedDataFrame["WAGE_RATE_OF_PAY_FROM"] = self.cleanedDataFrame["WAGE_RATE_OF_PAY_FROM"].str.replace(
            '[$,]', '', regex=True)
        self.cleanedDataFrame["WAGE_RATE_OF_PAY_TO"] = self.cleanedDataFrame["WAGE_RATE_OF_PAY_TO"].str.replace(
            '[$,]', '', regex=True)

        # Merge similar employer names to same name by removing ",", ",", "INC", "LLC" and put every name in upper case
        #self.cleanedDataFrame["EMPLOYER_NAME"] = self.cleanedDataFrame["EMPLOYER_NAME"].str.upper()
        #self.cleanedDataFrame["EMPLOYER_NAME"] = self.cleanedDataFrame["EMPLOYER_NAME"].str.replace(
        #    '[.,]|INC|LLC|LLP|LP|^\s+|\s+$|\s+(?=\s)', "", regex=True).rstrip()
        self.cleanedDataFrame["EMPLOYER_NAME"] = self.cleanedDataFrame["EMPLOYER_NAME"].apply(self.cleanEmployerName)
        
        # Mark major related job true, otherwise false
        self.cleanedDataFrame["IS_MAJOR_RELATED"] = self.cleanedDataFrame["SOC_CODE"].isin(self.majorSOC)

        # Delete all rows containing invalid values
        self.cleanedDataFrame.dropna()

        print("Size of table before cleaning: " + str(len(self.rawDataFrame)))
        print("Size of table after cleaning: " + str(len(self.cleanedDataFrame)))


    # Analyze and return the number of visas and the certificated rate
    def analyzeVisa(self, df: pandas.DataFrame):
        caseStatusRslt = df.groupby(["YEAR", "EMPLOYER_NAME", "CASE_STATUS"]).size().reset_index(
            name="CASE_STATUS_OCCURRENCE")
        caseStatusRslt = caseStatusRslt[caseStatusRslt["CASE_STATUS"] == self.analyzedVisaStatus]

        visaRslt = df.groupby(["YEAR", "EMPLOYER_NAME", "VISA_CLASS"]).size().reset_index(name="VISA_CLASS_OCCURRENCE")

        visaRslt = visaRslt[visaRslt["VISA_CLASS"] == self.analyzedVisaType]

        return caseStatusRslt, visaRslt

    def analyzeSalary(self, df: pandas.DataFrame):
        conversion_factors = {
            "Bi-Weekly": 26,  # Assuming there are 26 bi-weeks in a year
            "Week": 52,  # Assuming there are 52 weeks in a year
            "Hour": 2080,  # Assuming there are 2080 work hours in a year (40 hours per week * 52 weeks)
            "Month": 12,  # Assuming there are 12 months in a year
            "Year": 1,  # No need to change if unit is year
        }

        # Convert salaries to annual salary for units other than "Year"
        df["ANNUAL_MINIMUM_SALARY"] = df.apply(
            lambda row: float(row["WAGE_RATE_OF_PAY_FROM"]) * conversion_factors[row["WAGE_UNIT_OF_PAY"]], axis=1)

        salary = df.groupby(["YEAR", "EMPLOYER_NAME"])["ANNUAL_MINIMUM_SALARY"].median().reset_index(
            name="AVERAGE_MINIMUM_SALARY")
        return salary

    def analyzeLocation(self, df: pandas.DataFrame):
        locationRslt = df.groupby(["YEAR", "EMPLOYER_POSTAL_CODE"]).size().reset_index(
            name="EMPLOYER_POSTAL_CODE_OCCURRENCE")

        majorLocationRslt = df[df["SOC_CODE"].isin(self.majorSOC)]  # Extract jobs in given SOC
        majorLocationRslt = majorLocationRslt.groupby(["YEAR", "EMPLOYER_POSTAL_CODE"]).size().reset_index(
            name="MAJOR" + "_" + "EMPLOYER_POSTAL_CODE_OCCURRENCE")

        locationRslt = locationRslt.merge(majorLocationRslt, on=["YEAR", "EMPLOYER_POSTAL_CODE"])

        print("Median of job location occurrence is: " + str(locationRslt["EMPLOYER_POSTAL_CODE_OCCURRENCE"].median()))
        return locationRslt

    def generateBIReport(self):
        result = self.cleanedDataFrame.groupby(
            ["YEAR", "EMPLOYER_NAME", "EMPLOYER_POSTAL_CODE", "IS_MAJOR_RELATED", "CASE_STATUS",
                             "VISA_CLASS"]).size().rename('JOB_NUM')

        try:
            self.outputEmployerDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputEmployerDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        result.to_sql(self.outputEmployerDataTableName, self.outputEmployerDataDBEngine)
