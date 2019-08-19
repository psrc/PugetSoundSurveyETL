import pandas as pd
import logging
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader


class load():
    def __init__(self):      
        try:
            self.logger = logging.getLogger('surveyLogger')
            self.config = SurveyConfigReader.surveyConfig()
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def StageResponseFile(self, year, responseClass, responseFile):
        try:
            self.logger.info("Starting staging response file")
            with SurveyDatabase.surveyDatabase() as db:
        
                #read in where the header row starts
                header_row = int(self.config.get('Response',str(year)+responseClass+"header"))
                self.logger.info("Setting header row to: " + str(header_row))

                self.logger.info("Reading in file format for " + str(year))
                readFormat = self.config.get('Response',str(year)+responseClass+"format")

                if readFormat == 'excel':
                    self.logger.info('Reading in survey excel file from: ' + responseFile)
                    rfdf = pd.read_excel(responseFile, index_col=None, header=header_row)
                elif readFormat == 'database':
                    self.logger.info('Reading in survey from database table') 
                    rfdf = db.executeAndPandas("SELECT * FROM [stg].["+responseFile+"]")
                else:
                    raise Exception('Unknown Format Type')

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except:
            self.logger.error(e.args[0])
            raise

    def StageCodeBookFile(self, year, responseClass, codeBookFile):
        try:
            self.logger.info("Starting staging codebook file")

            with SurveyDatabase.surveyDatabase() as db:
                codebookHeaderRow = self.config.get('CodeBook',str(year)+responseClass+"header")
                codebookSheetName = self.config.get('CodeBook',str(year)+responseClass+"sheet")

                codebookDict = pd.read_excel(codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
                codebookDF = pd.DataFrame.from_dict(codebookDict)

                self.logger.info("Starting insertion of codebook file data into staging table.")
                db.createStagingTableFromDF(codebookDF,'codebook_'+responseClass+str(year))
                self.logger.info("Finished insertion of codebook file data into staging table.")

                self.logger.info("Starting transformation of codebook.")
                codebookDF = codebookDF.replace('Valid Values',pd.np.nan)
                codebookDF[['order','Field']] = codebookDF[['order','Field']].fillna(method='ffill')
                self.cbdf = codebookDF[['Field','Variable','Value']]
                self.logger.info("Finished transformation of codebook.")  
        except:
            self.logger.error(e.args[0])
            raise

    def getResponseDF(self):
        return self.rfdf

    def getCodeBookDF(self):
        return self.cbdf






