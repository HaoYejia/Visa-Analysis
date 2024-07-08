# This is a sample Python script.
import os
import re
import math
import numpy
import pathlib
import pandas
import fpdf

import sqlalchemy


from AnalyzeQuarterLCA import AnalyzeQuarterLCA
from GenerateLCAReport import GenerateLCAReport


class AnalyzeLCA():
    def __init__(self) -> None:

        self.majorName = "ECE"
        self.majorSOC = ["17-2070", "17-2071", "17-2071.00", "17-2072", "17-2072.00",
                         "17-2073", "17-2073.00", "17-2074", "17-2074.00", "17-2076", "17-2076.00"]

        self.analyzedVisaType = "H-1B"
        self.analyzedVisaStatus = "Certified"

        self.DBworkPath = "E:\\OneDrive - Rose-Hulman Institute of Technology\\Rose-Hulman\\Career\\H1bData\\H1bAnalysis\\Python Analysis 2\\"

        #self.DBworkPath = str(pathlib.Path().absolute())

        self.inputRawDataDBConfig = {
            'user': 'root',
            'password': '8LtM1zFleE9wIlMJ1F5M',
            'host': '10.47.240.3',
            'database': 'lca_raw_data',
        }

        self.inputDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.inputRawDataDBConfig['database'] + ".db")

        # self.inputDBEngine.connect().execute(sqlalchemy.text(""))
        self.outputEmployerDataDBConfig = {
            'user': 'root',
            'password': '8LtM1zFleE9wIlMJ1F5M',
            'host': '10.47.240.3',
            'database': 'lca_quarterly_analyzed_employer_data',
        }

        self.outputLocationDataDBConfig = {
            'user': 'root',
            'password': '8LtM1zFleE9wIlMJ1F5M',
            'host': '10.47.240.3',
            'database': 'lca_quarterly_analyzed_location_data',
        }

        self.outputEmployerDataDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath +  self.outputEmployerDataDBConfig[
                'database'] + ".db")

        self.outputLocationDataDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.outputLocationDataDBConfig[
                'database'] + ".db")

        self.startYear = 2020  # Inclusive
        self.endYear = 2023  # Inclusive

        self.outputResultDataDBConfig = {
            'user': 'root',
            'password': '8LtM1zFleE9wIlMJ1F5M',
            'host': '10.47.240.3',
            'database': 'lca_result',
        }

        self.outputResultDataDBEngine = sqlalchemy.create_engine(
            "sqlite:///" + self.DBworkPath + self.outputResultDataDBConfig[
                'database'] + ".db")
        
        self.combinedEmployerData = pandas.DataFrame()

    def analyzeQuarterLCA(self):
        tableNames = sqlalchemy.inspect(self.inputDBEngine).get_table_names()
        lcaTables = [table for table in tableNames if table.startswith("lca_disclosure_data")]

        # joblib.Parallel(n_jobs=multiprocessing.cpu_count(),prefer="threads")(joblib.delayed(self.analyzeQuarterLCAHelper)(table) for table in lcaTables)
        for table in lcaTables:
            analysis = AnalyzeQuarterLCA(inputTableName = table,
                                        majorName = self.majorName, 
                                        majorSOC = self.majorSOC, 
                                        VisaType = self.analyzedVisaType, 
                                        VisaStatus = self.analyzedVisaStatus, 
                                        DBPath = self.DBworkPath)
            analysis.cleanData()
            #analysis.generateReports()
            analysis.generateBIReport()

    '''
    def analyzeQuarterLCAHelper(self, table):
        analysis = AnalyzeQuarterLCA(table)
        analysis.cleanData()
        analysis.generateReports()
    '''

    def combineResults(self):
        employerTables = sqlalchemy.inspect(self.outputEmployerDataDBEngine).get_table_names()
        combinedEmployerData = pandas.DataFrame()

        # Read quarter employer related data
        combinedEmployerData = pandas.read_sql(
            self.createQueryString(employerTables, self.outputEmployerDataDBConfig['database']),
            self.outputEmployerDataDBEngine)

        # Sum employer data
        combinedEmployerData = combinedEmployerData.groupby(['YEAR', 'EMPLOYER_NAME']).agg({
            'MAJOR_RELATED_JOBS': 'sum',
            'TOTAL_JOBS': 'sum',
            'VISA_CLASS_OCCURRENCE': 'sum',
            'CASE_STATUS_OCCURRENCE': 'sum'
        }).reset_index()

        # Extract data between start and end year
        combinedEmployerData = combinedEmployerData[
            (combinedEmployerData['YEAR'] >= self.startYear) & (combinedEmployerData['YEAR'] <= self.endYear)]

        # Calcualte percentages
        combinedEmployerData.insert(loc=2, column="MAJOR_PERCENTAGE",
                                    value=(combinedEmployerData["MAJOR_RELATED_JOBS"] / combinedEmployerData[
                                        "TOTAL_JOBS"]) * 100)
        combinedEmployerData.insert(loc=5, column="CERTIFICATED_PERCENTAGE",
                                    value=(combinedEmployerData["CASE_STATUS_OCCURRENCE"] / combinedEmployerData[
                                        "TOTAL_JOBS"]) * 100)
        combinedEmployerData.insert(loc=7, column="H1B_PERCENTAGE",
                                    value=(combinedEmployerData["VISA_CLASS_OCCURRENCE"] / combinedEmployerData[
                                        "TOTAL_JOBS"]) * 100)

        # Export to database
        self.outputCombinedEmployerDataTableName = (
                    self.majorName + "_" + self.analyzedVisaType + "_" + self.analyzedVisaStatus + "_" + "combined_result").lower().replace(
            "-", "_")

        try:
            self.outputResultDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputCombinedEmployerDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        combinedEmployerData.to_sql(self.outputCombinedEmployerDataTableName, self.outputResultDataDBEngine)

        # Read quarter location related data from database
        locationTables = sqlalchemy.inspect(self.outputLocationDataDBEngine).get_table_names()
        combinedLocationData = pandas.DataFrame()

        combinedLocationData = pandas.read_sql(
            self.createQueryString(locationTables, self.outputLocationDataDBConfig['database']),
            self.outputLocationDataDBEngine)

        # Sum postal code occurance
        combinedLocationData = combinedLocationData.groupby(['YEAR', 'EMPLOYER_POSTAL_CODE']).agg({
            'EMPLOYER_POSTAL_CODE_OCCURRENCE': 'sum',
            'MAJOR_EMPLOYER_POSTAL_CODE_OCCURRENCE': 'sum',
        }).reset_index()

        # Convert to log scale
        combinedLocationData.insert(loc=3, column="EMPLOYER_POSTAL_CODE_OCCURRENCE_LOG",
                                    value=(10 * numpy.log10(combinedLocationData["EMPLOYER_POSTAL_CODE_OCCURRENCE"]) + 1))

        combinedLocationData.insert(loc=5, column="MAJOR_EMPLOYER_POSTAL_CODE_OCCURRENCE_LOG",
                                    value=(10 * numpy.log10(combinedLocationData["MAJOR_EMPLOYER_POSTAL_CODE_OCCURRENCE"]) + 1))

        combinedLocationData = combinedLocationData[
            (combinedLocationData['YEAR'] >= self.startYear) & (combinedLocationData['YEAR'] <= self.endYear)]

        self.outputCombinedLocationDataTableName = (self.majorName + "_" + "location_combined_result").lower().replace(
            "-", "_")

        try:
            self.outputResultDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputCombinedLocationDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        combinedLocationData.to_sql(self.outputCombinedLocationDataTableName, self.outputResultDataDBEngine)

        pass


    def combineBIResults(self):
        employerTables = sqlalchemy.inspect(self.outputEmployerDataDBEngine).get_table_names()
        

        # Read quarter employer related data
        self.combinedEmployerData = pandas.read_sql(
            self.createQueryString(employerTables, self.outputEmployerDataDBConfig['database']),
            self.outputEmployerDataDBEngine)
        
        # Sum employer data
        self.combinedEmployerData = self.combinedEmployerData.groupby(
            ["YEAR", "EMPLOYER_NAME", "EMPLOYER_POSTAL_CODE", "IS_MAJOR_RELATED", 
             "CASE_STATUS", "VISA_CLASS"]).sum().reset_index()
        
        
        # Process combined data
        resultEmployerData = pandas.DataFrame()

        resultEmployerData = self.combinedEmployerData.loc[:,"YEAR":"EMPLOYER_NAME"].drop_duplicates()

        
        return # skip generate more detailed data table
        # Options for different cases of combinations and their column names
        filterAndName = [[1, 'Certified','H-1B', "H1B_CER_MAJ_JOB_NUM"],
                         [1, 'Certified - Withdrawn', 'H-1B', "H1B_CERWTDR_MAJ_JOB_NUM"],
                         [1, 'Withdrawn', 'H-1B', "H1B_WTDR_MAJ_JOB_NUM"],
                         [1, 'Denied', 'H-1B', "H1B_DEN_MAJ_JOB_NUM"],

                         [1, 'Certified','E-3 Australian', "E3_CER_MAJ_JOB_NUM"],
                         [1, 'Certified - Withdrawn', 'E-3 Australian', "E3_CERWTDR_MAJ_JOB_NUM"],
                         [1, 'Withdrawn', 'E-3 Australian', "E3_WTDR_MAJ_JOB_NUM"],
                         [1, 'Denied', 'E-3 Australian', "E3_DEN_MAJ_JOB_NUM"],

                         [1, 'Certified','H-1B1 Singapore', "H1B1S_CER_MAJ_JOB_NUM"],
                         [1, 'Certified - Withdrawn', 'H-1B1 Singapore', "H1B1S_CERWTDR_MAJ_JOB_NUM"],
                         [1, 'Withdrawn', 'H-1B1 Singapore', "H1B1S_WTDR_MAJ_JOB_NUM"],
                         [1, 'Denied', 'H-1B1 Singapore', "H1B1S_DEN_MAJ_JOB_NUM"],

                         [1, 'Certified','H-1B1 Chile', "H1B1C_CER_MAJ_JOB_NUM"],
                         [1, 'Certified - Withdrawn', 'H-1B1 Chile', "H1B1C_CERWTDR_MAJ_JOB_NUM"],
                         [1, 'Withdrawn', 'H-1B1 Chile', "H1B1C_WTDR_MAJ_JOB_NUM"],
                         [1, 'Denied', 'H-1B1 Chile', "H1B1C_DEN_MAJ_JOB_NUM"],
                         
                         [0, 'Certified','H-1B', "H1B_CER_NONMAJ_JOB_NUM"],
                         [0, 'Certified - Withdrawn', 'H-1B', "H1B_CERWTDR_NONMAJ_JOB_NUM"],
                         [0, 'Withdrawn', 'H-1B', "H1B_WTDR_NONMAJ_JOB_NUM"],
                         [0, 'Denied', 'H-1B', "H1B_DEN_NONMAJ_JOB_NUM"],

                         [0, 'Certified','E-3 Australian', "E3_CER_NONMAJ_JOB_NUM"],
                         [0, 'Certified - Withdrawn', 'E-3 Australian', "E3_CERWTDR_NONMAJ_JOB_NUM"],
                         [0, 'Withdrawn', 'E-3 Australian', "E3_WTDR_NONMAJ_JOB_NUM"],
                         [0, 'Denied', 'E-3 Australian', "E3_DEN_NONMAJ_JOB_NUM"],

                         [0, 'Certified','H-1B1 Singapore', "H1B1S_CER_NONMAJ_JOB_NUM"],
                         [0, 'Certified - Withdrawn', 'H-1B1 Singapore', "H1B1S_CERWTDR_NONMAJ_JOB_NUM"],
                         [0, 'Withdrawn', 'H-1B1 Singapore', "H1B1S_WTDR_NONMAJ_JOB_NUM"],
                         [0, 'Denied', 'H-1B1 Singapore', "H1B1S_DEN_NONMAJ_JOB_NUM"],

                         [0, 'Certified','H-1B1 Chile', "H1B1C_CER_NONMAJ_JOB_NUM"],
                         [0, 'Certified - Withdrawn', 'H-1B1 Chile', "H1B1C_CERWTDR_NONMAJ_JOB_NUM"],
                         [0, 'Withdrawn', 'H-1B1 Chile', "H1B1C_WTDR_NONMAJ_JOB_NUM"],
                         [0, 'Denied', 'H-1B1 Chile', "H1B1C_DEN_NONMAJ_JOB_NUM"]]

        for option in filterAndName:
            selected = self.combinedEmployerData[ (self.combinedEmployerData['IS_MAJOR_RELATED'] == option[0]) & 
                                            (self.combinedEmployerData["CASE_STATUS"] == option[1]) & 
                                            (self.combinedEmployerData["VISA_CLASS"] == option[2])].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]

            resultEmployerData = resultEmployerData.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
            resultEmployerData = resultEmployerData.rename(columns = {"JOB_NUM" : option[3]})

        # Export Data
        self.outputCombinedEmployerDataTableName = (
                    self.majorName + "_" + "employer_combined_result").lower().replace(
            "-", "_")

        try:
            self.outputResultDataDBEngine.connect().execute(
                sqlalchemy.text("DROP TABLE " + self.outputCombinedEmployerDataTableName + "; "))
        except Exception as error:
            print("ERROR: ", error)

        resultEmployerData.to_sql(self.outputCombinedEmployerDataTableName, self.outputResultDataDBEngine)

    def generatePDFReport(self):
        self.outputCombinedEmployerDataTableName = (
                    self.majorName + "_" + "employer_combined_result").lower().replace(
            "-", "_")
        
        employerPDF = fpdf.FPDF()

        
        if (self.analyzedVisaType == "H-1B") & (self.analyzedVisaStatus == "Certified"):
            generator = GenerateLCAReport(self.combinedEmployerData, self.analyzedVisaType, self.analyzedVisaStatus, self.majorName)
            generator.generateEmployerGeneralDataFrame()
            generator.generateYearEmployerGeneralTable(2021)



    def createQueryString(self, tableNames, database):

        queryString = ""

        for table in tableNames:
            if table == tableNames[-1]:
                queryString += "SELECT * FROM " + table + "; "
            else:
                queryString += "SELECT * FROM " + table + " UNION "
        print(queryString)

        return queryString


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # path = "../GovData/"
    # print(os.listdir(path))

    # myReadLCA = AnalyzeQuarterLCA()

    # myReadLCA.cleanData()
    # myReadLCA.generateReports()

    myReadLCA = AnalyzeLCA()
    #myReadLCA.analyzeQuarterLCA()
    #myReadLCA.combineResults()
    myReadLCA.combineBIResults()
    myReadLCA.generatePDFReport()
