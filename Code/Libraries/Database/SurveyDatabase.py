"""
A single localize file to handle basic database connections, calls, etc..
"""
#pip install pyodbc
import pyodbc
import logging
import sys
import os
sys.path.append(os.path.abspath('./Code/')) #Allow internal testing
from Libraries.Logging import SurveyLogging
from Libraries.Configuration import SurveyConfigReader
#pip install xlrd
import pandas as pd
import sqlalchemy
import urllib

class surveyDatabase():
    def __init__(self):
        try:
            self.logger = logging.getLogger('surveyLogger')
            config = SurveyConfigReader.surveyConfig()
            self.server = config.get('SQLServer','SERVER')
            self.database = config.get('SQLServer','DATABASE')
            self.user = config.get('SQLServer','USER')
            self.password = config.get('SQLServer','PASSWORD')
            self.driver = config.get('SQLServer','DRIVER')
            self.sql_conn = pyodbc.connect("DRIVER={"+self.driver+"}; SERVER=" + self.server +"; DATABASE="+ self.database+"; UID="+self.user+"; PWD="+self.password)
            self.connStr = "DRIVER={"+self.driver+"}; SERVER=" + self.server +"; DATABASE="+ self.database+"; UID="+self.user+"; PWD="+self.password
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sql_conn.close()

    """
    Execute a query with no return dataset
    """
    def execute(self,query):
        try:
            cursor = self.sql_conn.cursor()
            cursor.execute(query)
            self.sql_conn.commit()
            cursor.close()
        except Exception as e:
            self.logger.error(e.args[0])
            self.logger.info(query)
            raise

    """
    Execute a query and returns a list
    """
    def executeAndFetch(self,query):
        try:
            cursor = self.sql_conn.cursor()
            cursor.execute(query)
            results = [item[0] for item in cursor.fetchall()]
            cursor.close()  
            return results
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    """
    Execute a query and returns a dataframe
    """
    def executeAndPandas(self,query):
        try:
            #cursor = self.sql_conn.cursor()
            results = pd.read_sql_query(query,self.sql_conn)
            #cursor.close()  
            return results
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    """
    Select a map column from the database
    """
    def selectMapColumn(self, map_col):
        try:
            query = "SELECT "+map_col+" FROM stg.Mapping_File WHERE "+map_col+" IS NOT NULL;"
            current_cols = self.executeAndFetch(query)
            return current_cols
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    """
    Creates a staging table given the map_col and a column list
    """
    def createStagingTableFromDF(self,df, name):
        try:
            params = urllib.parse.quote_plus(self.connStr)
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql(name=name, schema="stg", con=engine, if_exists="replace", index=False, chunksize=1000)
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def appendTableFromDF(self,schema,df, name):
        try:
            params = urllib.parse.quote_plus(self.connStr)
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql(name=name, schema=schema, con=engine, if_exists="append", index=False, chunksize=1000)
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def pullMappingTable(self,map_col):
        try:
            query = "SELECT ["+map_col+"] AS Orginal_Names,[Master_Names],[DataType] FROM [stg].[Mapping_File] WHERE ["+map_col+"] IS NOT NULL"
            return self.executeAndPandas(query)
        except Exception as e:
            self.logger.error(e.args[0])
            raise


"""
Used for testing logic
"""
if __name__ == '__main__':
    import datetime
    SurveyLogging.initLogging()

    #print('Internal testing of SurveyDatabase class')

    with surveyDatabase() as db:
        #print(db.server)
        #print(db.database)
        #print(db.user)

        #print('Starting map column logic')
        map_col = "file_2015"
        #db.selectMapColumn(map_col)
        #db.dropStagingTable(map_col)
        #print('Ending map column logic')


        path = "C:\\Users\\WilliamAndrus\\Datalere\\Marc Beacom - Datalere_Team\\Projects\\Puget Sound Regional Council\\Data Model\\Example ETL for Household Survey Data\\"

        filename = "2015-pr2-hhsurvey-person.xlsx"
        header_row = 0

        #print('Creating staging table')
        #sur = pd.read_excel(path + filename, index_col = None, header = header_row)
        sur = pd.read_excel(path + filename, index_col=None, header=header_row)  

        #start = datetime.now()
        db.createStagingTableFromDF(sur,"test")
        #end = datetime.now()

        #print(end-start)

        #db.insertIntoStagingTable(map_col,sur)

        #mappingDF = db.pullMappingTable(map_col)

        #Convert to Dictionary
        #mappingDict = mappingDF.set_index("Orginal_Names")["Master_Names"].to_dict()
        #sur = sur.rename(index=str, columns=mappingDict)
        #pip install openpyxl
        #sur.to_excel("output.xlsx")


