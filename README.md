# PugetSoundSurveyETL

**Master.py** is the main caller for the program. It is where the ETL steps/logic is located.
The first thing the program sets up is the logging; which, the program initially calls from the SurveyLogging file.

> **SurveyLogging.py** file sets up the logging level, logging print format, path, etc...

The Master program accepts 3 arguements when it runs from the command line or it reads from the launch.json file when running in VS Code.

1. is the *map_col*; which represents the column in the stg.MappingFile table in the database to be used for mapping current columns to master columns.
2. is the *path*; where the excel file can be found, if using them, and where the output files will be saved.
3. is the *filename*; this will either be the excel filename or the name of the database table representing the survey data. 

Next the Master uses the file/class called SurveyDatabase. 

> **SurveyDatabase.py** class defines basic excute query, creates/delete tables, and insert logic.

From there the Master program follows the basic layed out logic for reading the excel, database logic, etc.. for the ETL process.

Other notable files the program uses:

> **SurveyConfigReader.py** a very short program to read in the Config file: app.ini and to read values from that file.

> **app.ini** a text file with sections and key-value pairs that the program can then use for setting up basic static values. Within this file are the following sections:
> * SQLServer - where to store the username,password, server, database, and driver information
> * FilePaths - the path where the logging file will be saved
> * HeaderStarts - where in the excel file the header row is located, use -1 if there is no file as in the case of the database table.
> * Format - what format is the survey data in; with only 2 options currently programmed for: excel or database

> **settings.json** the python path to where the libraries have been downloaded to via *pip install*
