import pandas
import reportlab
from reportlab.platypus import SimpleDocTemplate, PageTemplate, Table, TableStyle, Paragraph, PageBreak
from reportlab.lib.units import inch
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
#from reportlab.platypus.flowables import LinkInPDF

class GenerateLCAReport():
    def __init__(self, combinedDataTable, visaType, visaStatus, majorName, jobNumLimit) -> None:
        self.combinedEmployerData = combinedDataTable
        self.analyzedVisaType = visaType
        self.analyzedVisaStatus = visaStatus
        self.majorName = majorName
        self.jobNumLimit = jobNumLimit

        self.countCategoryTable = self.countCategoryOfCombinedData(combinedDataTable)

        self.employerList = combinedDataTable["EMPLOYER_NAME"].drop_duplicates()
        self.elements = []
        self.pdfReport = SimpleDocTemplate("pdf_filename.pdf", pagesize=reportlab.lib.pagesizes.letter)
    '''
    def generateEmployerGeneralDataFrame(self):

        employerGeneralTable = pandas.DataFrame()
        employerGeneralTable =  self.combinedEmployerData.loc[:,"YEAR":"EMPLOYER_NAME"].drop_duplicates()

        
        self.generalCondition = ((self.combinedEmployerData['IS_MAJOR_RELATED'] == 1) & 
                                        (self.combinedEmployerData["CASE_STATUS"] == self.analyzedVisaStatus) & 
                                        (self.combinedEmployerData["VISA_CLASS"] == self.analyzedVisaType))
        
        selected = self.combinedEmployerData[self.generalCondition].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "H1B_CER_MAJ_JOB_NUM"})

        selected = self.combinedEmployerData[ ~self.generalCondition].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        selected = selected.groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "OTHER"})

        selected = self.combinedEmployerData.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "TOTAL"})

        employerGeneralTable = employerGeneralTable[employerGeneralTable["H1B_CER_MAJ_JOB_NUM"] >= self.jobNumLimit]

        employerGeneralTable= employerGeneralTable.astype({"OTHER" : int, "TOTAL": int, "H1B_CER_MAJ_JOB_NUM": int})

        self.employerList = employerGeneralTable["EMPLOYER_NAME"].drop_duplicates()
        return employerGeneralTable
    '''
    
    def employerNameToParagraph(self, name):
        return Paragraph(name)

    def generateYearEmployerGeneralTable(self, yearRange):
        #employerGeneralTable = self.generateEmployerGeneralDataFrame()

        for year in yearRange:
            print("Generating general table for year {}".format(year))

            yearEmployerGeneralTable = self.countCategoryTable[(self.countCategoryTable["YEAR"] == (year))]
            yearEmployerGeneralTable = yearEmployerGeneralTable[["YEAR", "EMPLOYER_NAME", "H1B_CER_MAJ_JOB_NUM","NEITHER_H1B_CER_MAJ_JOB_NUM",  "TOTAL"]]
            yearEmployerGeneralTable = yearEmployerGeneralTable[yearEmployerGeneralTable["H1B_CER_MAJ_JOB_NUM"] >= self.jobNumLimit]
            yearEmployerGeneralTable["EMPLOYER_NAME"] = yearEmployerGeneralTable["EMPLOYER_NAME"].apply(self.employerNameToParagraph)
            yearEmployerGeneralTable = yearEmployerGeneralTable.sort_values(by=["H1B_CER_MAJ_JOB_NUM"], ascending=False)

            
            
            styles = reportlab.lib.styles.getSampleStyleSheet()


            
            table = reportlab.platypus.Table([["Year", "Employer Name", self.majorName + " \n H-1B \n Certified", "Other", "Total"]] + yearEmployerGeneralTable.values.tolist())

            style = reportlab.platypus.TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), reportlab.lib.colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), reportlab.lib.colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), reportlab.lib.colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, reportlab.lib.colors.black),
            ])
            table.setStyle(style)

            self.elements.append(table)
            self.elements.append(PageBreak())
    
    def generateEmployerDetailedPages(self):

        

        for index, name in self.employerList.items():
            dataTable = self.countCategoryTable[self.countCategoryTable["EMPLOYER_NAME"] == name]

            # Do not create detailed page if total number of certified major jobs of this company never exceeds the limit
            if ((dataTable["H1B_CER_MAJ_JOB_NUM"] < self.jobNumLimit).all().all()):
                continue

            if (index > 1000):
                break

            print("Generating detailed page for {}".format(name))

            self.elements.append(Paragraph(name))

            majTypeStatusBarChart = VerticalBarChart()
            drawing = Drawing(400, 200)
            

            
            data = [
                    tuple(dataTable["H1B_CER_MAJ_JOB_NUM"].values.tolist()),
                    tuple(dataTable["NEITHER_H1B_CER_MAJ_JOB_NUM"].values.tolist())
                    ]


            majTypeStatusBarChart.data = data
            majTypeStatusBarChart.categoryAxis.style='stacked'
            majTypeStatusBarChart.categoryAxis.categoryNames = dataTable["YEAR"].astype(str).values.tolist()

            drawing.add(majTypeStatusBarChart)
            self.elements.append(drawing)

            self.elements.append(PageBreak())


    #def generateDrawing(self, drawing, )

    def countCategoryOfCombinedData(self, combinedData):

        resultEmployerData = pandas.DataFrame()

        resultEmployerData = combinedData.loc[:,"YEAR":"EMPLOYER_NAME"].drop_duplicates()

        selected = combinedData[ (combinedData['IS_MAJOR_RELATED'] == 1) & 
                                        (combinedData["CASE_STATUS"] == 'Certified') & 
                                        (combinedData["VISA_CLASS"] == 'H-1B')].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        selected = selected.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        resultEmployerData = resultEmployerData.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        resultEmployerData = resultEmployerData.rename(columns = {"JOB_NUM" : "H1B_CER_MAJ_JOB_NUM"})

        selected = combinedData[(combinedData["CASE_STATUS"] == 'Certified')].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        selected = selected.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        resultEmployerData = resultEmployerData.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        resultEmployerData = resultEmployerData.rename(columns = {"JOB_NUM" : "CER_JOB_NUM"})

        selected = combinedData[(combinedData["VISA_CLASS"] == 'H-1B')].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        selected = selected.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        resultEmployerData = resultEmployerData.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        resultEmployerData = resultEmployerData.rename(columns = {"JOB_NUM" : "H1B_JOB_NUM"})

        selected = combinedData.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        resultEmployerData = resultEmployerData.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        resultEmployerData = resultEmployerData.rename(columns = {"JOB_NUM" : "TOTAL"})


        resultEmployerData["NEITHER_H1B_CER_MAJ_JOB_NUM"] = resultEmployerData["TOTAL"] - resultEmployerData["H1B_CER_MAJ_JOB_NUM"]
        resultEmployerData["NOT_CER_JOB_NUM"] = resultEmployerData["TOTAL"] - resultEmployerData["CER_JOB_NUM"]
        resultEmployerData["NOT_H1B_JOB_NUM"] = resultEmployerData["TOTAL"] - resultEmployerData["H1B_JOB_NUM"]

        resultEmployerData = resultEmployerData.astype({ "H1B_CER_MAJ_JOB_NUM": int, "CER_JOB_NUM" : int, "TOTAL": int,
                                                        "H1B_JOB_NUM": int, "NEITHER_H1B_CER_MAJ_JOB_NUM": int, "NOT_CER_JOB_NUM": int,
                                                        "NOT_H1B_JOB_NUM": int})
        return resultEmployerData


    def export(self):
        self.pdfReport.build(self.elements)    