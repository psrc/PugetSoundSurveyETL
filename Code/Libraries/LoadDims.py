import pandas as pd
import logging
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader

class load():
    def __init__(self, year, responseClass):
        try:
            self.logger = logging.getLogger('surveyLogger')
            self.config = SurveyConfigReader.surveyConfig()
            self.year = year
            self.responseClass = responseClass
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def LoadDims(self):
        try:
            if self.responseClass == 'household':
                self.logger.info("Start loading HouseholdDim")
                #hhdf = rfdf[['hhid']] #create copy of dataframe for loading
                self.ProcessHouseHoldDim()
                self.logger.info("Finished loading HouseholdDim")
            elif self.responseClass == 'person':
                self.logger.info("Start loading PersonDim")
                dims.ProcessPersonDim()
                self.logger.info("Finished loading PersonDim")
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def ProcessHouseHoldDim(self):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.mergeHouseholdDim" + str(self.year))
                #upsert logic instead of sql logic
                #TODO: delete existing year data
                #TODO format df to HouseholdDim structure
                #TODO: append data to dim table
                #appendTableFromDF('dbo',df, 'HouseholdDim'):
            return True
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def TransformResponseAndCodeTable(self, rfdf, cbdf):
        #Foreach column in cbdf
        for header in list(rfdf.columns.values):
            rfdf = self.AddParseColumns(header, rfdf, cbdf)

        with SurveyDatabase.surveyDatabase() as db:
            db.createStagingTableFromDF(rfdf,'ResponseAndCode'+self.responseClass.capitalize()+'_'+str(self.year))


    def ProcessPersonDim(self):
        with SurveyDatabase.surveyDatabase() as db:
            db.execute("exec dbo.mergePersonDim" + self.responseClass.capitalize() + str(self.year))
            #upsert logic instead of sql logic


    def StandardizeCodebookColumn(self, colDF):
        try:
            if colDF.Variable.dtype == 'object':
                if colDF.Variable.str.isnumeric().max():
                    # the codebook values for fieldName are all numeric, so change them to int
                    colDF.Variable = colDF.Variable.astype(int)
            return colDF

        except Exception as e:
            self.logger.errors(e.args[0])
            raise


    def AddParseColumns(self, columnName, df, codebookDF):
        try:
            #pull column from code book
            colDF = codebookDF[ (codebookDF.Field == columnName) & (codebookDF.Variable.notnull()) ]
            colDF = self.StandardizeCodebookColumn(colDF)

            #Break out
            if columnName == 'student':
                colDF['studentind'] = colDF.apply(lambda s: 'N' if s['Value'] == 'No, not a student' else 'Y',axis=1)
            #Join new columns into response
            df = df.join(colDF.set_index('Variable'), on=columnName, how='left')
            # Rename columns
            df = df.rename(columns = {'Value':columnName+'Value','Field':columnName+'Field','Variable':columnName+'Variable'})
            # Drop column
            df = df.drop(columnName+'Field', axis=1)

            return df
        except Exception as e:
            print(e)
            self.logger.error(e.args[0])
            raise


    def ProcessMapping(self, df):
        with SurveyDatabase.surveyDatabase() as db:
            self.logger.info("Pulling in the mapping table")
            mappingDF = db.pullMappingTable("File_"+ str(self.year))

            #Convert to Dictionary
            mappingDict = mappingDF.set_index("Orginal_Names")["Master_Names"].to_dict()

            self.logger.info("Renaming column to master names")
            df = df.rename(index=str, columns=mappingDict)
        return True

