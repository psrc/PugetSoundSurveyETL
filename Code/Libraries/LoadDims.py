import pandas as pd
import numpy as np
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
            # MOREMORE park and ride columns should be defined in config file.
            self.park_and_ride_columns = ['park_ride_lot_start', 'park_ride_lot_end']
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
                self.ProcessPersonDim()
                self.logger.info("Finished loading PersonDim")
            elif self.responseClass == 'trip':
                self.logger.info("Start loading TripDim")
                self.ProcessTripDim()
                self.logger.info("Finished loading TripDim")
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
            db.execute("exec HHSurvey.mergePersonDim" + str(self.year))
            #upsert logic instead of sql logic


    def ProcessTripDim(self):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.merge_trip+dim_" + str(self.year))
                #upsert logic instead of sql logic
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def StandardizeCodebookColumn(self, colDF):

        def IsFloat(in_str):
            try:
                float(in_str)
                return True
            except:
                return False

        try:
            if colDF.Variable.dtype == 'object':
                if colDF.Variable.str.isnumeric().min():
                    # the codebook values for fieldName are all numeric, so change them to int
                    colDF.Variable = colDF.Variable.astype(int)
                elif colDF.Variable.str.isnumeric().max():
                    # some but not all the codebook values are numeric, so take the numeric ones
                    #moremore: isnumeric() doesn't think negatives are numeric!
                    colDF = colDF[colDF.Variable.apply(lambda x: IsFloat(x))]
                    colDF.Variable = colDF.Variable.astype(int)
                else:
                    #return 0-row dataframe but with Variable column as dtype int
                    colDF = colDF[colDF.Variable.apply(lambda x: type(x) != str)]
                    colDF.Variable = colDF.Variable.astype(int)
            return colDF

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def StandardizeDataColumn(self, column_name, rfdf):
        try:
            if rfdf[column_name].dtype == 'object':
                # replace whitespace values with nan's
                rfdf[column_name] = rfdf[column_name].replace(r'^\s*$', np.nan, regex=True)
            return rfdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def AddParseColumns(self, columnName, df, codebookDF):
        try:
            #pull column from code book
            colDF = codebookDF[ (codebookDF.Field == columnName) & (codebookDF.Variable.notnull()) ]
            colDF = self.StandardizeCodebookColumn(colDF)

            df = self.StandardizeDataColumn(columnName, df)

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
            self.logger.error("{} column: {}".format(self.responseClass, columnName))
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

