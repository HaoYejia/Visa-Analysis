import pandas
# import reportlab
# from reportlab.pdfgen import canvas
# from reportlab.platypus.flowables import Flowable
# from reportlab.lib.styles import ParagraphStyle as PS
# from reportlab.platypus import SimpleDocTemplate, PageTemplate, Table, TableStyle, Paragraph, PageBreak, Spacer, Image
# from reportlab.platypus.tableofcontents import TableOfContents
# from reportlab.lib.units import inch
# from reportlab.lib import colors
# from reportlab.graphics.shapes import Drawing
# from reportlab.graphics.charts.barcharts import VerticalBarChart
#from reportlab.platypus.flowables import LinkInPDF
import matplotlib.pyplot as plt
import tikzplotlib
# from io import BytesIO
# from svglib.svglib import svg2rlg
from pylatex import Command, Document, Section, Subsection, LongTable, Tabularx, Package
import re

class GenerateLCAReport():
    def __init__(self, combinedDataTable, visaType, visaStatus, majorName, jobNumLimit, startYear, endYear) -> None:
        self.combinedEmployerData = combinedDataTable
        self.analyzedVisaType = visaType
        self.analyzedVisaStatus = visaStatus
        self.majorName = majorName
        self.jobNumLimit = jobNumLimit
        self.startYear = startYear
        self.endYear = endYear

        self.countCategoryTable = self.countCategoryOfCombinedData(combinedDataTable)

        self.employerList = combinedDataTable["EMPLOYER_NAME"].drop_duplicates().sort_values()
        self.elements = []
        self.pdfReport = Document("basic")
        self.pdfReport.preamble.append(Command('usepackage', ['geometry']))
        self.pdfReport.preamble.append(Command('geometry', arguments='left=1in, right=1in, top=1in, bottom=1in'))


    
    def employerNameToParagraph(self, name):
        #return Paragraph("<a href=\"#{TAG}\"> {NAME} </a>".format(TAG=self.employerNameToLabel(name), NAME=name))
        return name

    def generateYearEmployerGeneralTable(self, yearRange):
        #employerGeneralTable = self.generateEmployerGeneralDataFrame()
        #styles = reportlab.lib.styles.getSampleStyleSheet()


        for year in yearRange:
            print("Generating general table for year {}".format(year))

            #self.elements.append(Paragraph("{YEAR} General Table".format(YEAR=year),style=styles['Title']))
            with self.pdfReport.create(Section("{YEAR} General Table".format(YEAR=year))):

                # Generate year general table
                yearEmployerGeneralTable = self.countCategoryTable[(self.countCategoryTable["YEAR"] == (year))]
                yearEmployerGeneralTable = yearEmployerGeneralTable[["YEAR", "EMPLOYER_NAME", "H1B_CER_MAJ_JOB_NUM","NEITHER_H1B_CER_MAJ_JOB_NUM",  "TOTAL"]]
                yearEmployerGeneralTable = yearEmployerGeneralTable[yearEmployerGeneralTable["H1B_CER_MAJ_JOB_NUM"] >= self.jobNumLimit]
                yearEmployerGeneralTable["EMPLOYER_NAME"] = yearEmployerGeneralTable["EMPLOYER_NAME"].apply(self.employerNameToParagraph)
                yearEmployerGeneralTable = yearEmployerGeneralTable.sort_values(by=["H1B_CER_MAJ_JOB_NUM"], ascending=False)

                
                # table = reportlab.platypus.Table([["Year", "Employer Name", self.majorName + " \n H-1B \n Certified", "Other", "Total"]] + yearEmployerGeneralTable.values.tolist())

                # style = reportlab.platypus.TableStyle([
                #     ('BACKGROUND', (0, 0), (-1, 0), reportlab.lib.colors.grey),
                #     ('TEXTCOLOR', (0, 0), (-1, 0), reportlab.lib.colors.whitesmoke),
                #     ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                #     ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                #     ('FONTSIZE', (0, 0), (-1, 0), 14),
                #     ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                #     ('BACKGROUND', (0, 1), (-1, -1), reportlab.lib.colors.beige),
                #     ('GRID', (0, 0), (-1, -1), 1, reportlab.lib.colors.black),
                # ])
                # table.setStyle(style)

                # self.elements.append(table)
                # self.elements.append(PageBreak())
                
                with self.pdfReport.create(LongTable(r"c|p{20em}|p{5em}|c|c")) as table:
                    table.add_hline()
                    table.add_row(["Year", "Employer Name", self.majorName + " \n H-1B \n Certified", "Other", "Total"])  # Add column names
                    table.add_hline()

                    for index, row in yearEmployerGeneralTable.iterrows():
                        table.add_row(row)
                        table.add_hline()

   
    def drawDetailedPlots(self, ax, num, fontSize, years, primaryData, primaryDataColor, primaryDataName, secondaryData, secondaryDataColor, secondaryDataName):
        # fig, ax = plt.subplots(figsize=size)
        ax[num].stackplot(years.values.tolist(),
                    primaryData.values.tolist(),
                    secondaryData.values.tolist(),
                    colors = [primaryDataColor, secondaryDataColor])
        ax[num].set_xticks(years.values.tolist())
        ax[num].legend([primaryDataName, secondaryDataName],fontsize=fontSize)

        # Save the plot to a BytesIO object
        # img_buffer = BytesIO()
        # plt.savefig(img_buffer, format='svg')
        # plt.close(fig)
        # img_buffer.seek(0)
        # drawing = svg2rlg(img_buffer)
        # return drawing
        
        

        

    def generateEmployerDetailedPages(self):
        # styles = reportlab.lib.styles.getSampleStyleSheet()

        for index, name in self.employerList.items():
            dataTable = self.countCategoryTable[self.countCategoryTable["EMPLOYER_NAME"] == name]

            # Do not create detailed page if total number of certified major jobs of this company never exceeds the limit
            if ((dataTable["H1B_CER_MAJ_JOB_NUM"] < self.jobNumLimit).all().all()):
                continue
            
            
            if (index > 1000):
               break
            

            print("Generating detailed page for {}".format(name))

            # Title of page
            # self.elements.append(Paragraph("<a name=\"{TAG}\"/>Detailed Page for {NAME}".format(NAME=name, TAG=self.employerNameToLabel(name)), styles['Title']))

            # fig, ax = plt.subplots(figsize=(5,2))
            # ax.stackplot(dataTable["YEAR"].values.tolist(),
            #             dataTable["H1B_CER_MAJ_JOB_NUM"].values.tolist(),
            #             dataTable["NEITHER_H1B_CER_MAJ_JOB_NUM"].values.tolist(),
            #             colors = ["#77AC30", "tab:gray"])
            # ax.set_xticks(dataTable["YEAR"].values.tolist())
            # ax.legend(["{} Certified H-1B Jobs".format(self.majorName), "Other Jobs"],fontsize=7)
            # # Save the plot to a BytesIO object
            # img_buffer = BytesIO()
            # plt.savefig(img_buffer, format='svg')
            # plt.close(fig)
            # img_buffer.seek(0)
            # drawing = svg2rlg(img_buffer)

            fig, ax = plt.subplots(nrows=3, figsize=(5.3,6))

            self.drawDetailedPlots(ax=ax, num=0, fontSize=7,
                                    years=dataTable["YEAR"],
                                    primaryData=dataTable["H1B_CER_MAJ_JOB_NUM"],
                                    primaryDataColor="#77AC30",
                                    primaryDataName="{} Certified H-1B Jobs".format(self.majorName),
                                    secondaryData=dataTable["NEITHER_H1B_CER_MAJ_JOB_NUM"],
                                    secondaryDataColor="tab:gray",
                                    secondaryDataName="Other Jobs")
            
            

            self.drawDetailedPlots(ax=ax, num=1, fontSize=7,
                                    years=dataTable["YEAR"],
                                    primaryData=dataTable["H1B_JOB_NUM"],
                                    primaryDataColor="#82B0D2",
                                    primaryDataName="H-1B Visa",
                                    secondaryData=dataTable["NOT_H1B_JOB_NUM"],
                                    secondaryDataColor="tab:gray",
                                    secondaryDataName="Other Visa")
            
            self.drawDetailedPlots(ax=ax, num=2, fontSize=7,
                                    years=dataTable["YEAR"],
                                    primaryData=dataTable["CER_JOB_NUM"],
                                    primaryDataColor="#82B0D2",
                                    primaryDataName="Certified",
                                    secondaryData=dataTable["NOT_CER_JOB_NUM"],
                                    secondaryDataColor="tab:gray",
                                    secondaryDataName="Not Certified")
            
            plt.savefig("test.pdf", format='pdf')
            return

            table = reportlab.platypus.Table([["Year", self.majorName + " H-1B Certified", "H-1B", "Certified" , "Total"]] + dataTable[["YEAR", "H1B_CER_MAJ_JOB_NUM", "H1B_JOB_NUM","CER_JOB_NUM","TOTAL"]].values.tolist())

            self.elements.append(Spacer(1,8))
            self.elements.append(table)

            tablestyle = reportlab.platypus.TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), reportlab.lib.colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), reportlab.lib.colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                #('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), reportlab.lib.colors.white),
                ('GRID', (0, 0), (-1, -1), 1, reportlab.lib.colors.black),
            ])
            table.setStyle(tablestyle)

            self.elements.append(PageBreak())
        return

    
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
        
        resultEmployerData = resultEmployerData[(resultEmployerData["YEAR"] >= self.startYear) & (resultEmployerData["YEAR"] <= self.endYear)]
        return resultEmployerData


    def export(self, name):
        print("Exporting PDF")
        self.pdfReport.generate_pdf(name, clean_tex=False)
        # self.pdfReport.generate_tex(name)

    def employerNameToLabel(self, name):
        return "" + str(re.sub("[^A-Za-z]","",name))
    
