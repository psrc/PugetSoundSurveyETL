import pandas as pd
import logging
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader


class load():
    def __init__(self, year, responseClass, responseFile, codeBookFile):
        try:
            self.logger = logging.getLogger('surveyLogger')
            self.config = SurveyConfigReader.surveyConfig()
            self.year = year
            self.responseClass = responseClass
            self.responseFile = responseFile
            self.codeBookFile = codeBookFile
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def StageResponseFile(self):
        try:
            self.logger.info("Starting staging response file")
            with SurveyDatabase.surveyDatabase() as db:

                #read in where the header row starts
                header_row = int(self.config.get('Response',str(self.year)+self.responseClass+"header"))
                self.logger.info("Setting header row to: " + str(header_row))
                sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                self.logger.info("Setting sheet name to: " + sheet_name)

                self.logger.info("Reading in file format for " + str(self.year))
                readFormat = self.config.get('Response',str(self.year)+self.responseClass+"format")

                if readFormat == 'excel':
                    response_sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                    self.logger.info("Setting sheet name to: " + response_sheet_name)
                    self.logger.info('Reading in survey excel file from: ' + self.responseFile)
                    rfdf = pd.read_excel(self.responseFile, index_col=None, header=header_row, sheet_name=response_sheet_name)
                elif readFormat == 'database':
                    self.logger.info('Reading in survey from database table')
                    rfdf = db.executeAndPandas("SELECT * FROM [stg].["+self.responseFile+"]")
                else:
                    raise Exception('Unknown Format Type')

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(self.year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def StageCodeBookFile(self):

        def ReproduceModeChoiceSets(df):
            try:
                self.logger.info("Reproducing mode choice lookups")
                mode_choice_df = df[df.Field == 'mode_4']
                for i in [1,2,3]:
                    mode_choice_df = mode_choice_df.assign(Field='mode_' + str(i))
                    df = df.append(mode_choice_df)
                return df

            except Exception as e:
                self.logger.error(e.args[0])
                raise

        try:
            self.logger.info("Starting staging codebook file")

            with SurveyDatabase.surveyDatabase() as db:
                codebookHeaderRow = self.config.get('CodeBook',str(self.year)+self.responseClass+"header")
                codebookSheetName = self.config.get('CodeBook',str(self.year)+self.responseClass+"sheet")

                codebookDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
                codebookDF = pd.DataFrame.from_dict(codebookDict)

                self.logger.info("Starting insertion of codebook file data into staging table.")
                db.createStagingTableFromDF(codebookDF,'codebook_'+self.responseClass+str(self.year))
                self.logger.info("Finished insertion of codebook file data into staging table.")

                self.logger.info("Starting transformation of codebook.")
                codebookDF = codebookDF.replace('Valid Values',pd.np.nan)
                codebookDF = codebookDF.replace('Labeled Values',pd.np.nan)
                codebookDF[['order','Field']] = codebookDF[['order','Field']].fillna(method='ffill')
                if (self.year == '2017' and self.responseClass == 'trip'):
                    codebookDF = ReproduceModeChoiceSets(codebookDF)
                self.cbdf = codebookDF[['Field','Variable','Value']]
                self.logger.info("Finished transformation of codebook.")
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def getResponseDF(self):
        return self.rfdf

    def getCodeBookDF(self):
        return self.cbdf






