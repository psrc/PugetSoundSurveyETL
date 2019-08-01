import sys
import logging
import pandas as pd
import os
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader


def ProcessResponseFile():
    with SurveyDatabase.surveyDatabase() as db:
        logger.info("Reading column mapping for " + map_col +" from the database")
        current_cols = db.selectMapColumn(map_col)

        #read in where the header row starts
        header_row = int(config.get('Response',str(year)+"header"))
        logger.info("Setting header row to: " + str(header_row))

        logger.info("Reading in file format for " + str(year))
        readFormat = config.get('Response',str(year)+"format")

        if readFormat == 'excel':
            logger.info('Reading in survey excel file from: ' + responseFile)
            sur = pd.read_excel(responseFile, index_col=None, header=header_row)
        elif readFormat == 'database':
            logger.info('Reading in survey from database table') 
            sur = db.executeAndPandas("SELECT * FROM [stg].["+responseFile+"]")
        else:
            raise Exception('Unknown Format Type')

        return sur


    return True

def ProcessCodeBookFile():
    with SurveyDatabase.surveyDatabase() as db:
        codebookHeaderRow = config.get('CodeBook',str(year)+"header")
        codebookSheetName = config.get('CodeBook',str(year)+"sheet")

        codebookDict = pd.read_excel(codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
        codebookDF = pd.DataFrame.from_dict(codebookDict)
        codebookDF = codebookDF.replace('Valid Values',pd.np.nan)
        codebookDF[['order','Field']] = codebookDF[['order','Field']].fillna(method='ffill')

        return codebookDF[['Field','Variable','Value']]

    return True

def ProcessMapping():
    with SurveyDatabase.surveyDatabase() as db:
        logger.info("Pulling in the mapping table")
        mappingDF = db.pullMappingTable(map_col)

        #Convert to Dictionary
        mappingDict = mappingDF.set_index("Orginal_Names")["Master_Names"].to_dict()

        logger.info("Renaming column to master names")
        sur = sur.rename(index=str, columns=mappingDict)
    return True

def ProcessDimTables():
    with SurveyDatabase.surveyDatabase() as db:
        db.execute("exec dbo.mergePersonDim" +str(year))
        db.execute("exec dbo.mergeHouseHoldDim" + str(year))
    return True

def ProcessFactTables():
    with SurveyDatabase.surveyDatabase() as db:
        db.execute("exec dbo.mergePersonFact" +str(year))
    return True

if __name__ == '__main__' :
    SurveyLogging.initLogging()
    logger = logging.getLogger('surveyLogger')

    logger.info("Master Started")

    try :
        if len(sys.argv) > 1:
            year = sys.argv[1]
            responseFile = sys.argv[2]
            codeBookFile = sys.argv[3]
        else:
            print("Send in 3 string arguments:  year, response file's path, and code book's path  or edit the launch.json file if debugging locally")
            raise Exception('Missing arguments')
        
        map_col = "file_" + str(year)

        #initialize config reader to pull values
        config = SurveyConfigReader.surveyConfig()

        cbdf = ProcessCodeBookFile()
        rfdf = ProcessResponseFile()


        #######################################################################################
        #TODO: Create Function to do (Create SubTable => Join => Rename => Drop logic)   
        # ParseColumn(ColumnName)
        ########################################################################################

        #
        # rfdf
        #
        #pull sub tables
        resptypeDF = cbdf[(cbdf.Field == 'resptype') & (cbdf.Variable >= 0)]
        #Join new columns into response
        rfdf = rfdf.join(resptypeDF.set_index('Variable'), on='resptype', how='left')
        # Rename columns
        rfdf = rfdf.rename(columns = {'Value':'respTypeValue','Field':'respTypeField','Variable':'respTypeVariable'})
        # Drop column
        rfdf = rfdf.drop('respTypeField', axis=1)

        
        #
        # student
        #
        #pull sub tables        
        studentDF = cbdf[(cbdf.Field == 'student') & (cbdf.Variable >= 0)]
        
        #TODO: split out questions with studentind
        # IF: No, not a student => 'N'
        # ELSE: 'Y'

        #Join new columns into response
        rfdf = rfdf.join(studentDF.set_index('Variable'), on='resptype', how='left')
        # Rename columns
        rfdf = rfdf.rename(columns = {'Value':'studentValue','Field':'studentField','Variable':'studentVariable'})
        # Drop column
        rfdf = rfdf.drop('studentField', axis=1)



        #Create/Replace new tables
        with SurveyDatabase.surveyDatabase() as db:
            #Upload data to table before merge
            logger.info("Starting insertion of code book into staging table...")
            db.createStagingTableFromDF(cbdf,str(year)+"CodeBook")

            logger.info("Starting insertion of data into staging table...")
            db.createStagingTableFromDF(rfdf,'Survey_file_'+str(year))


        #ProcessMapping() #dependent on ProcessResponseFile
        
        ProcessDimTables()

        ProcessFactTables()
  
    except Exception as e:
        logger.error(e.args[0])
    
    logger.info("Master Ended")
