import pandas
import matplotlib.pyplot as plt
import tikzplotlib
from pylatex import Command, Document, Section, Subsection, LongTable, Tabularx, Package, Figure, NoEscape, Hyperref, Marker, Label
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
        self.pdfReport.preamble.append(Command('usepackage', ['hyperref']))
        self.pdfReport.preamble.append(Command('geometry', arguments='left=1in, right=1in, top=1in, bottom=1in'))

        # Add table of Contents
        self.pdfReport.append(Command('tableofcontents'))
        self.pdfReport.append(Command('newpage'))

    def generateYearEmployerGeneralTable(self, yearRange):
        #employerGeneralTable = self.generateEmployerGeneralDataFrame()
        #styles = reportlab.lib.styles.getSampleStyleSheet()

        with self.pdfReport.create(Section("General Table")):

            for year in yearRange:
                print("Generating general table for year {}".format(year))

                #self.elements.append(Paragraph("{YEAR} General Table".format(YEAR=year),style=styles['Title']))
                with self.pdfReport.create(Subsection("{YEAR} General Table".format(YEAR=year))):

                    # Generate year general table
                    yearEmployerGeneralTable = self.countCategoryTable[(self.countCategoryTable["YEAR"] == (year))]
                    yearEmployerGeneralTable = yearEmployerGeneralTable[["YEAR", "EMPLOYER_NAME", "H1B_CER_MAJ_JOB_NUM","NEITHER_H1B_CER_MAJ_JOB_NUM",  "TOTAL"]]
                    yearEmployerGeneralTable = yearEmployerGeneralTable[yearEmployerGeneralTable["H1B_CER_MAJ_JOB_NUM"] >= self.jobNumLimit]
                    yearEmployerGeneralTable["EMPLOYER_NAME"] = yearEmployerGeneralTable["EMPLOYER_NAME"].apply(self.employerNameToRef)
                    yearEmployerGeneralTable = yearEmployerGeneralTable.sort_values(by=["H1B_CER_MAJ_JOB_NUM"], ascending=False)

                    
                    with self.pdfReport.create(LongTable(r"c|p{20em}|p{5em}|c|c")) as table:
                        table.add_hline()
                        table.add_row(["Year", "Employer Name", self.majorName + " \n H-1B \n Certified", "Other", "Total"])  # Add column names
                        table.add_hline()

                        for index, row in yearEmployerGeneralTable.iterrows():
                            table.add_row(row)
                            table.add_hline()

                self.pdfReport.append(Command('newpage'))

   
    def drawDetailedPlots(self, ax, num, fontSize, years, primaryData, primaryDataColor, primaryDataName, secondaryData, secondaryDataColor, secondaryDataName):
        # fig, ax = plt.subplots(figsize=size)
        ax[num].stackplot(years.values.tolist(),
                    primaryData.values.tolist(),
                    secondaryData.values.tolist(),
                    colors = [primaryDataColor, secondaryDataColor])
        ax[num].set_xticks(years.values.tolist())
        ax[num].legend([primaryDataName, secondaryDataName],fontsize=fontSize)


    def generateEmployerDetailedPages(self):
        # styles = reportlab.lib.styles.getSampleStyleSheet()

        tempImgPath = "./temp_img/"
        
        # Do not create detailed page if total number of certified major jobs of this company never exceeds the limit
        # First, create a boolean Series for each employer whether they exceed the jobNumLimit or not
        exceedsLimit = self.countCategoryTable.groupby('EMPLOYER_NAME')['H1B_CER_MAJ_JOB_NUM'].max() >= self.jobNumLimit
        # Filter out the employers who do not exceed the limit
        validEmployers = exceedsLimit[exceedsLimit].index

        # limit numbers of employers for faster debug
        # validEmployers = validEmployers[:10]
            
        with self.pdfReport.create(Section("Employer Detail")):

            # Create detailed pages only for those employers who meet the criteria
            for name in validEmployers:

                dataTable = self.countCategoryTable[self.countCategoryTable["EMPLOYER_NAME"] == name]

                print("Generating detailed page for {}".format(name))

                fig, ax = plt.subplots(nrows=3, figsize=(5.3,5.5))

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
                
                detailedImgName = "{filename}.pdf".format(filename=(self.employerNameToLabel(name) + "img"))
                                                        
                plt.savefig((tempImgPath + detailedImgName), format='pdf')
                plt.clf()
                plt.close('all')

                with self.pdfReport.create(Subsection(name + ":")):
                    self.pdfReport.append(Label(Marker(self.employerNameToLabel(name))))

                    with self.pdfReport.create(Figure(position="htbp")) as plot:
                        plot.add_image(filename=(tempImgPath + detailedImgName), width=NoEscape(r"0.9\textwidth"))
                    
                    with self.pdfReport.create(LongTable(r"c|c|c|c|c")) as table:
                        table.add_hline()
                        table.add_row(["Year", self.majorName + " H-1B Certified", "H-1B", "Certified" , "Total"])  # Add column names
                        table.add_hline()

                        for index, row in dataTable[["YEAR", "H1B_CER_MAJ_JOB_NUM", "H1B_JOB_NUM","CER_JOB_NUM","TOTAL"]].iterrows():
                            table.add_row(row)
                            table.add_hline()

                self.pdfReport.append(Command('newpage'))

    
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
        # self.pdfReport.generate_pdf(name, clean_tex=False)
        self.pdfReport.generate_tex(name)

    def employerNameToLabel(self, name):
        return "" + str(re.sub("[^A-Za-z0-9]","",name)) + "_detailed"
    
    def employerNameToRef(self, name):
        # return NoEscape(r"" + r"\ref{" + self.employerNameToLabel(name)  + r"}")
        return Hyperref(marker=Marker(name, prefix="subsec"), text=name)
    
