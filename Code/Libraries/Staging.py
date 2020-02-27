import pandas as pd
import logging
from itertools import product
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
                elif readFormat == 'csv':
                    self.logger.info('Reading in survey csv file from: ' + self.responseFile)
                    rfdf = pd.read_csv(self.responseFile, index_col=None, header=header_row)
                else:
                    raise Exception('Unknown Format Type')

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(self.year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def ReproduceModeChoiceSets(self, cbdf):
        try:
            self.logger.info("Reproducing mode choice lookups")
            mode_choice_df = cbdf[cbdf.Field == 'mode_4']
            for i in [1,2,3]:
                mode_choice_df = mode_choice_df.assign(Field='mode_' + str(i))
                cbdf = cbdf.append(mode_choice_df)
            return cbdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddParkAndRideRowsToCodebook(self, cbdf):
        try:
            prHeaderRow = self.config.get('CodeBook', str(self.year)+'parkandrideheader')
            prSheetName = self.config.get('CodeBook', str(self.year)+'parkandridesheet')
            prDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(prHeaderRow), sheet_name=prSheetName)
            prDF = pd.DataFrame.from_dict(prDict)
            prDF.columns=['Field', 'Variable', 'Value']
            self.logger.info("inserting park and ride data into cbdf")
            for col_name in self.park_and_ride_columns:
                prDF.Field = col_name
                cbdf = cbdf[cbdf.Field != col_name] #remove the single-line bogus junk record from codebook
                cbdf = cbdf.append(prDF)
            return cbdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddSpecialCodebookSheet (self, cbdf, sheet_type):
        try:
            self.logger.info("Beginning AddSpecialCodebookSheet() for {}".format(sheet_type))
            response_cols = self.config.get('Response', str(self.year)+sheet_type+'columns').split(', ')
            special_cols = self.config.get('CodeBook', str(self.year)+sheet_type+'columns').split(', ')
            column_ids = list(range(0, len(special_cols)))
            headerRow = self.config.get('CodeBook', str(self.year)+sheet_type+'header')
            sheetName = self.config.get('CodeBook', str(self.year)+sheet_type+'sheet')
            specialDict = pd.read_excel(self.codeBookFile, 
                                        index_col=None, 
                                        header=int(headerRow), 
                                        sheet_name=sheetName, 
                                        usecols=column_ids)
            specialDF = pd.DataFrame.from_dict(specialDict)
            specialDF.columns = special_cols
            specialDF = specialDF[['Field', 'Variable', 'Value']]
            self.logger.info("inserting special sheet data into codebook for {} data.".format(sheet_type))
            for col_name in response_cols:
                specialDF.Field = col_name
                self.logger.info('Removing bogus junk records from codebook')
                cbdf = cbdf[cbdf.Field != col_name] #remove the single-line bogus junk record from codebook
                self.logger.info("Appending specialDF for column {}".format(col_name))
                #cbdf = cbdf.append(specialDF, sort=True)
                cbdf = pd.concat([cbdf, specialDF], sort=True)
                self.logger.info("Finished appending specialDF for column {}".format(col_name))
            return cbdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddSpecialCodeBookRows(self, cbdf):
        try:
            cbdf = self.ReproduceModeChoiceSets(cbdf)
            cbdf = self.AddSpecialCodebookSheet(cbdf, 'parkandride')
            cbdf = self.AddSpecialCodebookSheet(cbdf, 'transitline')
            return cbdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def StageCodeBookFile(self):

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
                    codebookDF = self.AddSpecialCodeBookRows(codebookDF)
                self.cbdf = codebookDF[['Field','Variable','Value']]
                self.logger.info("Finished transformation of codebook.")
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def getResponseDF(self):
        return self.rfdf

    def getCodeBookDF(self):
        return self.cbdf


class load2019(load):

    def __init__(self, year, responseClass, responseFile, codeBookFile):
        super().__init__(year, responseClass, responseFile, codeBookFile)


    def StageCodeBookFile(self):

        try:
            self.logger.info("Starting staging codebook file")

            codebookHeaderRow = self.config.get('CodeBook',str(self.year)+self.responseClass+"header")
            codebookSheetName = self.config.get('CodeBook',str(self.year)+self.responseClass+"sheet")
            codebookDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
            codebookDF = pd.DataFrame.from_dict(codebookDict)

            self.logger.info("Starting transformation of codebook.")
            codebookDF = self.AddCodesForMissingData(codebookDF)
            codebookDF = codebookDF.drop_duplicates()
            self.cbdf = codebookDF

            self.logger.info("Starting insertion of codebook file data into staging table.")
            with SurveyDatabase.surveyDatabase() as db:
                db.createStagingTableFromDF(self.cbdf,'codebook_'+self.responseClass+str(self.year))

            self.logger.info("Finished insertion of codebook file data into staging table.")

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddCodesForMissingData(self, codebookDF):

        try:
            self.logger.info("Starting Staging.load2019.AddCodesForMissingData.")
            df_missing_data = codebookDF[codebookDF.variable == '_All categorical variables_'][['value', 'label']]
            df_alternate_missing_data = pd.DataFrame({'value': [-995], 'label':['Missing: Skip logic']}) # some data was mistakenly coded -995 instead of 995
            df_missing_data = df_missing_data.append(df_alternate_missing_data)
            df_main = codebookDF[codebookDF.variable != '_All categorical variables_']
            variables_list = df_main.variable.unique()
            df_variables = pd.DataFrame({'variable':variables_list})

            # 995 is a valid value for arrival_time_mam and depart_time_mam, so don't add skip logic lookup value for them.
            df_variables = df_variables[df_variables.variable != 'arrival_time_mam']
            df_variables = df_variables[df_variables.variable != 'depart_time_mam']

            combolist = list(product(df_variables.variable, df_missing_data.value))
            df_combinations = pd.DataFrame(data=combolist, columns=['variable','value'])
            df_full_emptydataset = pd.merge(df_combinations, df_missing_data, how='inner', on='value' )
            df_fullset = pd.concat([df_full_emptydataset,df_main])
            df_fullset = df_fullset.rename(columns={'variable':'Field','value':'Variable','label':'Value'})
            self.logger.info("Finishing Staging.load2019.AddCodesForMissingData.")
            return df_fullset

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
                    rfdf = pd.read_excel(self.responseFile,
                            index_col=None,
                            header=header_row,
                            sheet_name=response_sheet_name)
                elif readFormat == 'database':
                    self.logger.info('Reading in survey from database table')
                    rfdf = db.executeAndPandas("SELECT * FROM [stg].["+self.responseFile+"]")
                elif readFormat == 'csv':
                    self.logger.info('Reading in survey csv file from: ' + self.responseFile)
                    rfdf = pd.read_csv(self.responseFile,
                        index_col=None,
                        header=header_row,
                        encoding='ISO-8859-1')
                else:
                    raise Exception('Unknown Format Type')

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(self.year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise
