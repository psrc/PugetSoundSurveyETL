"""
A single placeholder for calling the logging logic setup

"""

import logging
import logging.config
import datetime
import SurveyConfigReader

def initLogging():
    config = SurveyConfigReader.surveyConfig()
    path = config.get('FilePaths','SurveyDirectory')

    #DEBUG - Detailed information, typically of interest only when diagnosing problems.
    #INFO - Confirmation that things are working as expected.
    #WARNING - An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
    #ERROR - Due to a more serious problem, the software has not been able to perform some function.
    #CRITICAL - A serious error, indicating that the program itself may be unable to continue running.
    loggingLevel = logging.INFO
    for logger_name in ['surveyLogger']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(loggingLevel)
        ch = logging.FileHandler(path + 'Logging_'+  datetime.datetime.now().strftime("%Y%m%d") +'.log')
        ch.setLevel(loggingLevel)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
        logger.addHandler(ch)

"""
Used for testing locally to see if file runs
"""
if __name__ == '__main__':
    initLogging()
    # create logger
    logger = logging.getLogger('surveyLogger')

    # 'application' code
    logger.debug('debug message')
    logger.info('info message')
    logger.warning('warn message')
    logger.error('error message')
    logger.critical('critical message')