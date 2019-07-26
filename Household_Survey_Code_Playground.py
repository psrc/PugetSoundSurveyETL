# %% Import libraries
import pandas as pd
import numpy as np
import datetime
import pyodbc

# %% Save the script start runtime
masterstart = datetime.datetime.now()

# %% Manually specify the name of the column in the mapping file for the
# data file to run
map_col = "file_2015"

# %% Set up the connection to the Azure Database
server = "datalere.database.windows.net"
database = "Datalere_Demo"
user = "Datalere_Demo_DB_Admin"
password = "e3nl%ByKxUF92^1VS%&eDC9CxL%&"
sql_conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=" +
                          server+"; DATABASE="+database+"; UID="+user+"; PWD="+password)

# %% Pull in file_YYYY from mapping file and search for the column names
# specified in the file to automatically locate the header row
# save as a var to use later
cursor = sql_conn.cursor()
query = "SELECT "+map_col+" FROM stg.Mapping_File WHERE "+map_col+" IS NOT NULL;"
cursor.execute(query)
current_cols = [item[0] for item in cursor.fetchall()]

# COME BACK TO THIS LATER AFTER FIGURING OUT WHETHER Chris gets the file
# in another format or in Excel
header_row = 0

# %% Import Excel Files for Now
# Question - how does Chris actually access this data?
# Can extraction of the data be automated? Automation is
# a best practice.
path = "C:\\Users\\KaraAnnanie\\Datalere\\Marc Beacom - Datalere_Team\\Projects\\Puget Sound Regional Council\\Data Model\\Example ETL for Household Survey Data\\"
#filename = "2017-pr2-2-person.xlsx"
filename = "2015-pr2-hhsurvey-person.xlsx"

#sur = pd.read_excel(path + filename, index_col = None, header = header_row)
sur = pd.read_excel(path + filename, index_col=None, header=header_row)

# %% Drop staging table if exists
cursor = sql_conn.cursor()
query = "DROP TABLE IF EXISTS stg.Survey_"+map_col+";"
cursor.execute(query)
cursor.close()

# %% Create string query to create table
query = "CREATE TABLE stg.Survey_"+map_col+" ("

for item in sur.columns:
    query = query + " [" + item + "] VARBINARY(MAX)"
    if item != sur.columns[len(sur.columns)-1]:
        query = query + ","
    else:
        query = query + ")"

# %% Create table in SQL Database
cursor = sql_conn.cursor()
cursor.execute(query)
cursor.commit()
cursor.close()

# %% Insert rows into newly created staging table (THIS NEEDS TO BE DONE YET)


sql_conn.close()


# %% Use a loop, astype, and a dictionary that extracts data types
# dynamically from the SQL target


# %% Pull in the mapping file
# (I don't have the authority to create a table
# in PSRC's SQL Server so I am pulling it in as
# an Excel sheet for now)
# filenameM = "Data_Mapping_2015_2017.xlsx"

# mapping = pd.read_excel(path + filenameM, index_col=None,
#                         header=0, sheet_name="Mapping_Table")
# file_2017 = (name for name in mapping["file_2017"])
# file_2015 = (name for name in mapping["file_2015"])

#Replace this with a query from datalere_demo database mapping_file, but still need to be able to 
#construct the dictionary below so we can rename columns in the dataframe to use the Master_Name column names

mapping = dict(zip(map_col, Master_Name))

#Will need a where statement to limit master names to only columns that also exist in map_col

# %% Rename the columns in the survey file to be the same as the master names, which are more descriptive
sur = sur.rename(index=str, columns=mapping)
