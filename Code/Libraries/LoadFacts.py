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


    def ProcessPersonFactTable(self, personFactDF):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.mergePersonFact" + str(self.year))
            return True
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def ProcessHouseholdFactTable(self):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.mergeHouseholdFact" + str(self.year))
            return True
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def LoadFacts(self, rfdf):
        try:
            if self.responseClass == 'household':
                self.logger.info("Start loading HouseholdFact")
                #self.ProcessHouseHoldFact()
                self.logger.info("Households exist as dimentions only.  No fact tables to load.")
            elif self.responseClass == 'person':
                self.logger.info("Start processing PersonFact")
                personFactDF = rfdf[['personid','hhid','numtrips','diary_duration_minutes']]
                self.ProcessPersonFactTable(personFactDF)
                self.logger.info("finished processing PersonFact")
        except Exception as e:
            self.logger.error(e.args[0])
            raise
