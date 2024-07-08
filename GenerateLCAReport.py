import pandas
import reportlab
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
#from reportlab.platypus.flowables import LinkInPDF

class GenerateLCAReport():
    def __init__(self, combinedDataTable, visaType, visaStatus, majorName) -> None:
        self.combinedEmployerData = combinedDataTable
        self.analyzedVisaType = visaType
        self.analyzedVisaStatus = visaStatus
        self.majorName = majorName


    def generateEmployerGeneralDataFrame(self):

        employerGeneralTable = pandas.DataFrame()
        employerGeneralTable =  self.combinedEmployerData.loc[:,"YEAR":"EMPLOYER_NAME"].drop_duplicates()

        selected = self.combinedEmployerData[ (self.combinedEmployerData['IS_MAJOR_RELATED'] == 1) & 
                                        (self.combinedEmployerData["CASE_STATUS"] == self.analyzedVisaStatus) & 
                                        (self.combinedEmployerData["VISA_CLASS"] == self.analyzedVisaType)].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "H1B_CER_MAJ_JOB_NUM"})

        selected = self.combinedEmployerData[ ~ ((self.combinedEmployerData['IS_MAJOR_RELATED'] == 1) & 
                                        (self.combinedEmployerData["CASE_STATUS"] == self.analyzedVisaStatus) & 
                                        (self.combinedEmployerData["VISA_CLASS"] == self.analyzedVisaType))].loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]]
        selected = selected.groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "OTHER"})

        selected = self.combinedEmployerData.loc[:,["YEAR", "EMPLOYER_NAME", "JOB_NUM"]].groupby(["YEAR", "EMPLOYER_NAME"]).sum().reset_index()
        employerGeneralTable = employerGeneralTable.merge(selected, how = 'outer', on = ["YEAR", "EMPLOYER_NAME"]).fillna(0)
        employerGeneralTable = employerGeneralTable.rename(columns = {"JOB_NUM" : "TOTAL"})

        employerGeneralTable= employerGeneralTable.astype({"OTHER" : int, "TOTAL": int, "H1B_CER_MAJ_JOB_NUM": int})

        return employerGeneralTable

    def generateYearEmployerGeneralTable(self, yearRange):
        employerGeneralTable = self.generateEmployerGeneralDataFrame()

        for year in [2021]:
            yearEmployerGeneralTable = employerGeneralTable[(employerGeneralTable["YEAR"] == (year))]
            yearEmployerGeneralTable = yearEmployerGeneralTable.nlargest(200, "H1B_CER_MAJ_JOB_NUM")
            

            pdf = SimpleDocTemplate("pdf_filename.pdf", pagesize=reportlab.lib.pagesizes.letter)
            elements = []
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

            elements.append(table)

            pdf.build(elements)