import sys
import logging
import pandas as pd
import os
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader
from Libraries import Staging
from Libraries import LoadDims
from Libraries import LoadFacts


if __name__ == '__main__' :

    #MASTER -> STAGE -> DIM -> FACT
    #           |         |     |
    #           |         |     Person FAct
    #           |         |Person DIm
    #           |         |HH Dim
    #           |Process files into mem
    #           |--send each dim/fact a subset of the df
    #
    # Loading result; count loaded into dim, count loaded fact; and dangling  dimension items

    SurveyLogging.initLogging()
    logger = logging.getLogger('surveyLogger')
    logger.info("Master Started")

    try :
        if len(sys.argv) > 1:
            year = sys.argv[1]
            responseClass = sys.argv[2].lower()
            responseFile = sys.argv[3]
            codeBookFile = sys.argv[4]
        else:
            print("Send in 4 string arguments:  year, response class (person, household, trip), response file's path, and code book's path  or edit the launch.json file if debugging locally")
            raise Exception('Missing arguments')

        #initialize config reader to pull values
        config = SurveyConfigReader.surveyConfig()

        """
        STAGING
        """
        logger.info("Starting Staging")
        stg = Staging.load(year, responseClass, responseFile, codeBookFile)

        logger.info("Staging response file")
        stg.StageResponseFile()

        logger.info("Staging codebook file")
        stg.StageCodeBookFile()
        rfdf = stg.getResponseDF()

        cbdf = stg.getCodeBookDF()
        logger.info("Finished Staging")

        """
        DIM LOADING
        """
        logger.info("Starting Dim Loading")
        dims = LoadDims.load(year, responseClass)

        logger.info("Start transforming new table")
        dims.TransformResponseAndCodeTable(rfdf, cbdf)
        logger.info("Finished tranforming tables")

        logger.info("Start loading Dimensions")
        dims.LoadDims()
        logger.info("Finished Dim Loading")


        """
        FACT LOADING
        """
        logger.info("Starting Fact Loading")
        fact = LoadFacts.load(year, responseClass)

        fact.LoadFacts(rfdf)
        #logger.info("Start processing PersonFact")
        #personFactDF = rfdf[['personid','hhid','numtrips','diary_duration_minutes']]
        #fact.ProcessPersonFactTable(personFactDF)
        #logger.info("Finished processing PersonFact")


        logger.info("Finished Fact Loading")

    except Exception as e:
        logger.error(e.args[0])

    logger.info("Master Ended")
