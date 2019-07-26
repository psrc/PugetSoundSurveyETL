# PugetSoundSurveyETL

**Master.py** is the main caller for the program. It's where the ETL steps/logic is located.
The first thing it sets up is the logging, which it initially calls the SurveyLogging file.

> **SurveyLogging.py** file sets up the logging level, logging print format, path, etc...

The Master program accepts 3 arguements when it runs from the command line or reads from the launch.json file when running in VS Code.

1. is the map_col; which represents the column in the stg.MappingFile table in the database to be used.
2. is the path; where the excel files can be found if using them and where the output files will be saved.
3. is the filename. This will either be the excel filename or the name of the database table. 

Next the Master uses the file/class called SurveyDatabase. 

> **SurveyDatabase.py** class defines basic excute query, creates/delete tables, and insert logic.

From there is follows basic logic for reading the excel and database logic for the ETL process.

Other files the program uses:

> **SurveyConfigReader.py** a very short program to read in the Config file: app.ini and to read values from the file.

> **app.ini** a text file with section and key-value pairs that the program can then use. Within this file are the following sections:
> * SQLServer - where to store the username,password, server, database, and driver information
> * FilePaths - the path where the logging file will be saved
> * HeaderStarts - where in the excel file the header row is located, use -1 if there is no file.
> * Format - what format is the survey data in with only 2 options programmed in: excel or database

> **settings.json** the python path to where the libraries have been downloaded to via *pip install*



