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

    def ProcessFactTable(self):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.merge_" + self.responseClass + "_fact_" + str(self.year))
            return True
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def LoadFacts(self, rfdf):
        try:
            if self.responseClass == 'household':
                self.logger.info("Start loading HouseholdFact")
                #self.ProcessHouseHoldFact()
                self.ProcessHouseholdFactTable()
                self.logger.info("Finished processing HouseholdFact.")
            elif self.responseClass == 'person':
                self.logger.info("Start processing PersonFact")
                personFactDF = rfdf[['personid','hhid','numtrips','diary_duration_minutes']]
                self.ProcessFactTable()
                self.logger.info("finished processing PersonFact")
            if self.responseClass == 'trip':
                self.logger.info("Start loading TripFact")
                self.ProcessFactTable()
        except Exception as e:
            self.logger.error(e.args[0])
            raise

class load2019(load):

    def __init__(self, year, responseClass):
        super().__init__(year, responseClass)


    def ProcessHouseholdFactTable(self):
        try:
            with SurveyDatabase.surveyDatabase() as db:
                db.execute("exec HHSurvey.merge_household_fact_" + str(self.year))
            return True
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def LoadFacts(self, rfdf):
        try:
            if self.responseClass == 'household':
                self.logger.info("Start loading HouseholdFact")
                #self.ProcessHouseHoldFact()
                self.ProcessHouseholdFactTable()
                self.logger.info("Finished processing HouseholdFact.")
            elif self.responseClass == 'person':
                self.logger.info("Person data resides in dimensions only;  No fact table generated. ")
            if self.responseClass == 'trip':
                self.logger.info("Start loading TripFact")
                self.ProcessFactTable()
        except Exception as e:
            self.logger.error(e.args[0])
            raise
