"""
A single placeholder for calling the logging logic setup

"""

import logging
import logging.config
import datetime
import sys
import os
sys.path.append(os.path.abspath('./Code/')) #Allow internal testing
from Libraries.Configuration import SurveyConfigReader
from Libraries.Database import SurveyDatabase

class LogDBHandler(logging.Handler):
    '''
    Customized logging handler that puts logs to the database.
    pymssql required
    '''
    def __init__(self, sql_conn, sql_cursor, db_tbl_log):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn
        self.db_tbl_log = db_tbl_log

    def emit(self, record):
        # Set current time
        tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Clear the log message so it can be put to db via sql (escape quotes)
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')
        # Make the SQL insert
        sql = 'INSERT INTO ' + self.db_tbl_log + ' ([log_level]' +\
            ', [log_levelname], [log], [lineno], module, [pathname], [created_at], [created_by]) ' + \
            'VALUES (' + \
            ''   + str(record.levelno) + ', ' + \
            '\'' + str(record.levelname) + '\', ' + \
            '\'' + str(self.log_msg) + '\', ' + \
            '\'' + str(record.lineno) + '\', ' + \
            '\'' + str(record.module) + '\', ' + \
            '\'' + str(record.pathname) + '\', ' + \
            '(convert(datetime2(7), \'' + tm + '\')), ' + \
            '\'' + str(record.name) + '\')'
        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the database :)
        except:
            print(sql)
            print('CRITICAL DB ERROR! Logging to database not possible!')

def initLogging():
    config = SurveyConfigReader.surveyConfig()
    LogToDB = config.get('Logging','LogToDB')


    #DEBUG - Detailed information, typically of interest only when diagnosing problems.
    #INFO - Confirmation that things are working as expected.
    #WARNING - An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
    #ERROR - Due to a more serious problem, the software has not been able to perform some function.
    #CRITICAL - A serious error, indicating that the program itself may be unable to continue running.
    loggingLevel = logging.INFO
    for logger_name in ['surveyLogger']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(loggingLevel)
        
        if LogToDB == 'True':
            sqlConn = SurveyDatabase.surveyDatabase().sql_conn
            cursor = sqlConn.cursor()
            ch = LogDBHandler(sqlConn,cursor,"stg.log")
        else:
            path = config.get('Logging','LoggingDirectory')
            ch = logging.FileHandler(path + 'Logging_'+  datetime.datetime.now().strftime("%Y%m%d") +'.log')
            ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
        
        ch.setLevel(loggingLevel)       
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