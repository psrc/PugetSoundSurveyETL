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

    def ProcessPersonFactTable(self):
        with SurveyDatabase.surveyDatabase() as db:
            db.execute("exec dbo.mergePersonFact" +str(year))

        return True

