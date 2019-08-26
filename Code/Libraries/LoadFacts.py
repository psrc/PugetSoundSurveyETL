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
        with SurveyDatabase.surveyDatabase() as db:
            db.execute("exec dbo.mergePersonFact" + self.responseClass.capitalize() + str(self.year))

        return True

