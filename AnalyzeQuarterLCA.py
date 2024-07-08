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

        }

        # Raw, unprocessed dataframe
        # self.rawDataFrame = pandas.DataFrame(columns=self.essentialCol)

        self.inputDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.inputRawDataDBConfig['database'] + ".db")

        self.outputEmployerDataDBEngine = sqlalchemy.create_engine(
           "sqlite:///" + self.DBworkPath + self.outputEmployerDataDBConfig[
                'database'] + ".db")

        self.outputLocationDataDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.outputLocationDataDBConfig[
                'database'] + ".db")

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
        self.cleanedDataFrame["EMPLOYER_NAME"] = self.cleanedDataFrame["EMPLOYER_NAME"].str.upper()
        self.cleanedDataFrame["EMPLOYER_NAME"] = self.cleanedDataFrame["EMPLOYER_NAME"].str.replace(
            '[.,]|INC|LLC|LLP|LP|^\s+|\s+$|\s+(?=\s)', "", regex=True)
        
        # Mark major related job true, otherwise false
        self.cleanedDataFrame["IS_MAJOR_RELATED"] = self.cleanedDataFrame["SOC_CODE"].isin(self.majorSOC)

        # Delete all rows containing invalid values
        self.cleanedDataFrame.dropna()

        print("Size of table before cleaning: " + str(len(self.rawDataFrame)))
        print("Size of table after cleaning: " + str(len(self.cleanedDataFrame)))

    '''
    def extractEmployer(self, df: pandas.DataFrame):
        employers = df["EMPLOYER_NAME"].unique()
        return employers

    def extractSOC(self, df: pandas.DataFrame, soc: list):
        jobs = df[df["SOC_CODE"].isin(soc)]  # Extract jobs in given SOC
        return jobs
    '''

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

    # Analyze and return the number of major related jobs provided by the employer
    def analyzeEmployer(self, df: pandas.DataFrame):
        jobs = df[df["SOC_CODE"].isin(self.majorSOC)]  # Extract jobs in given SOC
        jobs = jobs.groupby(["YEAR", "EMPLOYER_NAME"]).size().reset_index(name="MAJOR_RELATED_JOBS")

        return jobs

    def generateEmployerReport(self):
        certificated, h1b = self.analyzeVisa(self.cleanedDataFrame)
        # certificated = certificated[certificated["CASE_STATUS"] == "Certified"]
        # h1b = h1b[h1b["VISA_CLASS"] == "H-1B"]

        totalJobs = self.cleanedDataFrame.groupby(["YEAR", "EMPLOYER_NAME"]).size().reset_index(name="TOTAL_JOBS")
        majorJobs = self.analyzeEmployer(self.cleanedDataFrame)

        # salary = self.analyzeSalary(self.cleanedDataFrame)

        # Merging tables
        result = majorJobs.merge(totalJobs, on=["YEAR", "EMPLOYER_NAME"])
        result = result.merge(certificated, on=["YEAR", "EMPLOYER_NAME"])
        result = result.merge(h1b, on=["YEAR", "EMPLOYER_NAME"])
        # result = result.merge(salary, on=["YEAR", "EMPLOYER_NAME"])

        # Calculated percentages
        '''
        result.insert(loc=2, column="MAJOR_PERCENTAGE",
                      value=(result["MAJOR_RELATED_JOBS"] / result["TOTAL_JOBS"]) * 100)
        result.insert(loc=5, column="CERTIFICATED_PERCENTAGE",
                      value=(result["CASE_STATUS_OCCURRENCE"] / result["TOTAL_JOBS"]) * 100)
        result.insert(loc=7, column="H1B_PERCENTAGE",
                      value=(result["VISA_CLASS_OCCURRENCE"] / result["TOTAL_JOBS"]) * 100)
        '''

        try:
            self.outputEmployerDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputEmployerDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        result.to_sql(self.outputEmployerDataTableName, self.outputEmployerDataDBEngine)

    def generateLocationReport(self):
        try:
            self.outputLocationDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputLocationDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        self.analyzeLocation(self.cleanedDataFrame).to_sql(self.outputLocationDataTableName,
                                                           self.outputLocationDataDBEngine)

    def generateReports(self):

        self.generateEmployerReport()
        self.generateLocationReport()

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
