import sys
import logging
import pandas as pd
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader



if __name__ == '__main__' :
    SurveyLogging.initLogging()
    logger = logging.getLogger('surveyLogger')

    logger.info("Master Started")

    try :
        if len(sys.argv) > 1:
            map_col = sys.argv[1]
            path = sys.argv[2]
            filename = sys.argv[3]
        else:
            print("Send in 3 string arguments:  map_col, path, filename  or edit the launch.json file if debugging locally")
            raise Exception('Missing arguments')
        


        ##TODO: READ in code book.
        # Fill data down
        # df.fillna(....)
        # populate table with 
        # values, response text, data types, etc.... 
        #


        with SurveyDatabase.surveyDatabase() as db:
            logger.info("Reading column mapping for " + map_col +" from the database")
            current_cols = db.selectMapColumn(map_col)

            #initialize config reader to pull values
            config = SurveyConfigReader.surveyConfig()

            #read in where the header row starts
            header_row = int(config.get('HeaderStarts',map_col))
            logger.info("Setting header row to: " + str(header_row))

            logger.info("Reading in file format for " + map_col)
            readFormat = config.get('Format',map_col)

            if readFormat == 'excel':
                logger.info('Reading in survey excel file from: ' + path + filename)
                sur = pd.read_excel(path + filename, index_col=None, header=header_row)
            elif readFormat == 'database':
                logger.info('Reading in survey from database table') 
                sur = db.executeAndPandas("SELECT * FROM [stg].["+filename+"]")
            else:
                raise Exception('Unknown Format Type')

            logger.info("Dropping staging table for " + map_col)
            db.dropStagingTable(map_col)

            logger.info("Creating staging table for " + map_col)
            db.createStagingTable(map_col,sur)

            logger.info("Starting insertion of data into staging table...")
            db.insertIntoStagingTable(map_col,sur)

            

            logger.info("Pulling in the mapping table")
            mappingDF = db.pullMappingTable(map_col)
            
        #Convert to Dictionary
        mappingDict = mappingDF.set_index("Orginal_Names")["Master_Names"].to_dict()

        logger.info("Renaming column to master names")
        sur = sur.rename(index=str, columns=mappingDict)



        #TODO: Export to Database
        #logger.info("Save results to excel")
        #sur.to_excel(map_col+"_output.xlsx")



    except Exception as e:
        logger.error(e.args[0])
    
    logger.info("Master Ended")
