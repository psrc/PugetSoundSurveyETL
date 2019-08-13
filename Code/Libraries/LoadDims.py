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

    def ProcessHouseHoldDim(self, year, df):
        with SurveyDatabase.surveyDatabase() as db:
            db.execute("exec dbo.mergeHouseHoldDim" + str(year))
            #upsert logic instead of sql logic
            #TODO: delete existing year data
            #TODO format df to HouseholdDim structure
            #TODO: append data to dim table
            #appendTableFromDF('dbo',df, 'HouseholdDim'):
        return True
    
    def TransformResponseAndCodeTable(self, year, rfdf, cbdf):
        #Foreach column in cbdf
        for header in list(rfdf.columns.values):
            rfdf = self.AddParseColumns(header,rfdf, cbdf)

        with SurveyDatabase.surveyDatabase() as db:
            db.createStagingTableFromDF(rfdf,'ResponseAndCode_'+str(year))

    def ProcessPersonDim(self,year, df, cbdf):
        with SurveyDatabase.surveyDatabase() as db:
            db.execute("exec dbo.mergePersonDim" +str(year))
            #upsert logic instead of sql logic
            
   
    def AddParseColumns(self, columnName, df, codebookDF):
        #pull column from code book
        colDF = codebookDF[(codebookDF.Field == columnName) & (codebookDF.Variable >= 0)]

        #Break out
        if columnName == 'student':
            colDF['studentind'] = colDF.apply(lambda s: 'N' if s['Value'] == 'No, not a student' else 'Y',axis=1)
        #elif columnName == ''
    

        #Join new columns into response
        df = df.join(colDF.set_index('Variable'), on=columnName, how='left')
        # Rename columns
        df = df.rename(columns = {'Value':columnName+'Value','Field':columnName+'Field','Variable':columnName+'Variable'})
        # Drop column
        df = df.drop(columnName+'Field', axis=1)

        return df


    def ProcessMapping(self, year, df):
        with SurveyDatabase.surveyDatabase() as db:
            self.logger.info("Pulling in the mapping table")
            mappingDF = db.pullMappingTable("File_"+ str(year))

            #Convert to Dictionary
            mappingDict = mappingDF.set_index("Orginal_Names")["Master_Names"].to_dict()

            self.logger.info("Renaming column to master names")
            df = df.rename(index=str, columns=mappingDict)
        return True

        