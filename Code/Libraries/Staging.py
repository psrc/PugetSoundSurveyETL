import pandas as pd
import logging
from itertools import product
from Libraries.Logging import SurveyLogging
from Libraries.Database import SurveyDatabase
from Libraries.Configuration import SurveyConfigReader
from shapely.wkt import dumps, loads
from geopandas import GeoDataFrame


class load():
    def __init__(self, year, responseClass, responseFile, codeBookFile):
        try:
            self.logger = logging.getLogger('surveyLogger')
            self.config = SurveyConfigReader.surveyConfig()
            self.year = year
            self.responseClass = responseClass
            self.responseFile = responseFile
            self.codeBookFile = codeBookFile
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def StageResponseFile(self):
        try:
            self.logger.info("Starting staging response file")
            with SurveyDatabase.surveyDatabase() as db:


                self.logger.info("Reading in file format for " + str(self.year))
                readFormat = self.config.get('Response',str(self.year)+self.responseClass+"format")

                if readFormat == 'excel':
                    #read in where the header row starts
                    header_row = int(self.config.get('Response',str(self.year)+self.responseClass+"header"))
                    self.logger.info("Setting header row to: " + str(header_row))
                    sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                    self.logger.info("Setting sheet name to: " + sheet_name)
                    response_sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                    self.logger.info("Setting sheet name to: " + response_sheet_name)
                    self.logger.info('Reading in survey excel file from: ' + self.responseFile)
                    rfdf = pd.read_excel(self.responseFile, index_col=None, header=header_row, sheet_name=response_sheet_name)
                elif readFormat == 'database':
                    self.logger.info('Reading in survey from database table')
                    response_query = self.create_response_query()
                    rfdf = db.executeAndPandas(response_query)
                elif readFormat == 'csv':
                    #read in where the header row starts
                    header_row = int(self.config.get('Response',str(self.year)+self.responseClass+"header"))
                    self.logger.info("Setting header row to: " + str(header_row))
                    self.logger.info('Reading in survey csv file from: ' + self.responseFile)
                    rfdf = pd.read_csv(self.responseFile, index_col=None, header=header_row)
                else:
                    raise Exception('Unknown Format Type')

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(self.year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def create_response_query(self):
        '''
        create a query that selects all columns from self.responseFile,
        but which transforms columns of type GEOMETRY to 
        '''
        try:
            with SurveyDatabase.surveyDatabase() as db:
                table_name = self.responseFile
                col_sql = "SELECT COLUMN_NAME FROM {} WHERE TABLE_NAME = '{}' \
                    and COLUMN_NAME <> 'hhmember12' \
                    and DATA_TYPE not in ('GEOGRAPHY', 'GEOMETRY')".format(
                    '[hhts_cleaning].INFORMATION_SCHEMA.COLUMNS',
                    table_name)
                col_df = db.executeAndPandas(col_sql)
                l = []
                for n in col_df.COLUMN_NAME:
                    l.append(n) if n not in ['GDB_GEOMATTR_DATA', 'SDE_STATE_ID'] else l
                geog_col_sql = "SELECT COLUMN_NAME FROM {} WHERE TABLE_NAME = '{}' \
                    and DATA_TYPE in ('GEOGRAPHY', 'GEOMETRY')".format(
                    '[hhts_cleaning].INFORMATION_SCHEMA.COLUMNS',
                    table_name)
                geog_col_df = db.executeAndPandas(geog_col_sql)
                for n in geog_col_df.COLUMN_NAME:
                    col_selector = n + '.ToString() as ' + n + '_str'
                    l.append(col_selector) if n not in ['GDB_GEOMATTR_DATA', 'SDE_STATE_ID'] else l
                columns_clause = ','.join(l)
                response_query = 'select {} from [hhts_cleaning].[HHSurvey].{}'.format(
                    columns_clause,
                    table_name
                )
            return response_query

        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def ReproduceModeChoiceSets(self, cbdf):
        try:
            self.logger.info("Reproducing mode choice lookups")
            mode_choice_df = cbdf[cbdf.Field == 'mode_4']
            for i in [1,2,3]:
                mode_choice_df = mode_choice_df.assign(Field='mode_' + str(i))
                cbdf = cbdf.append(mode_choice_df)
            return cbdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddParkAndRideRowsToCodebook(self, cbdf):
        try:
            prHeaderRow = self.config.get('CodeBook', str(self.year)+'parkandrideheader')
            prSheetName = self.config.get('CodeBook', str(self.year)+'parkandridesheet')
            prDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(prHeaderRow), sheet_name=prSheetName)
            prDF = pd.DataFrame.from_dict(prDict)
            prDF.columns=['Field', 'Variable', 'Value']
            self.logger.info("inserting park and ride data into cbdf")
            for col_name in self.park_and_ride_columns:
                prDF.Field = col_name
                cbdf = cbdf[cbdf.Field != col_name] #remove the single-line bogus junk record from codebook
                cbdf = cbdf.append(prDF)
            return cbdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddSpecialCodebookSheet (self, cbdf, sheet_type):
        try:
            self.logger.info("Beginning AddSpecialCodebookSheet() for {}".format(sheet_type))
            response_cols = self.config.get('Response', str(self.year)+sheet_type+'columns').split(', ')
            special_cols = self.config.get('CodeBook', str(self.year)+sheet_type+'columns').split(', ')
            column_ids = list(range(0, len(special_cols)))
            headerRow = self.config.get('CodeBook', str(self.year)+sheet_type+'header')
            sheetName = self.config.get('CodeBook', str(self.year)+sheet_type+'sheet')
            specialDict = pd.read_excel(self.codeBookFile, 
                                        index_col=None, 
                                        header=int(headerRow), 
                                        sheet_name=sheetName, 
                                        usecols=column_ids)
            specialDF = pd.DataFrame.from_dict(specialDict)
            specialDF.columns = special_cols
            specialDF = specialDF[['Field', 'Variable', 'Value']]
            self.logger.info("inserting special sheet data into codebook for {} data.".format(sheet_type))
            for col_name in response_cols:
                specialDF.Field = col_name
                self.logger.info('Removing bogus junk records from codebook')
                cbdf = cbdf[cbdf.Field != col_name] #remove the single-line bogus junk record from codebook
                self.logger.info("Appending specialDF for column {}".format(col_name))
                #cbdf = cbdf.append(specialDF, sort=True)
                cbdf = pd.concat([cbdf, specialDF], sort=True)
                self.logger.info("Finished appending specialDF for column {}".format(col_name))
            return cbdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddSpecialCodeBookRows(self, cbdf):
        try:
            cbdf = self.ReproduceModeChoiceSets(cbdf)
            cbdf = self.AddSpecialCodebookSheet(cbdf, 'parkandride')
            cbdf = self.AddSpecialCodebookSheet(cbdf, 'transitline')
            return cbdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def StageCodeBookFile(self):

        try:
            self.logger.info("Starting staging codebook file")

            with SurveyDatabase.surveyDatabase() as db:
                codebookHeaderRow = self.config.get('CodeBook',str(self.year)+self.responseClass+"header")
                codebookSheetName = self.config.get('CodeBook',str(self.year)+self.responseClass+"sheet")

                codebookDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
                codebookDF = pd.DataFrame.from_dict(codebookDict)

                self.logger.info("Starting insertion of codebook file data into staging table.")
                db.createStagingTableFromDF(codebookDF,'codebook_'+self.responseClass+str(self.year))
                self.logger.info("Finished insertion of codebook file data into staging table.")

                self.logger.info("Starting transformation of codebook.")
                codebookDF = codebookDF.replace('Valid Values',pd.np.nan)
                codebookDF = codebookDF.replace('Labeled Values',pd.np.nan)
                #codebookDF[['order','Field']] = codebookDF[['order','Field']].fillna(method='ffill')
                #codebookDF.columns = ['Field', 'Variable', 'Value', 'field_a', 'field_b']
                codebookDF.columns = ['Field', 'Variable', 'Value']
                if (self.year == '2017' and self.responseClass == 'trip'):
                    codebookDF = self.AddSpecialCodeBookRows(codebookDF)
                self.cbdf = codebookDF[['Field','Variable','Value']]
                self.logger.info("Finished transformation of codebook.")

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def add_stateplane_wkt(self, rfdf):
        """
        Chenge this to work from a dataframe instead of reading from SQL
        """
        def reproject_column_wgs84_to_stateplane(rfdf, in_pk, in_col):
            try: 
                self.logger.info("starting reproject_column_wgs84_to_stateplane")
                IN_SRID = '4326'
                self.logger.info("OUT_SRID")
                OUT_SRID = '2285'
                self.logger.info("geometry...")
                geometry = [loads(x) for x in rfdf[in_col]]
                self.logger.info("gdf_in = ...")
                gdf_in = GeoDataFrame(rfdf[in_pk], geometry=geometry, crs='EPSG:' + IN_SRID)
                self.logger.info('type(gdf_in) = '+ str(type(gdf_in)))
                self.logger.info('type(geometry[2]) = '+ str(type(geometry[2])))
                self.logger.info('type(gdf_in) = '+ str(type(gdf_in)))
                self.logger.info("gdf_reproj = ...")
                #gdf_reproj = gdf_in.to_crs(epsg = int(OUT_SRID))
                gdf_reproj = gdf_in.to_crs(epsg=2285)
                self.logger.info("wkt_reproj = ...")
                wkt_reproj = [dumps(x) for x in gdf_reproj.geometry]
                if 'wgs84' in in_col:
                    wkt_field_header = in_col.replace('wgs84', OUT_SRID)
                else:
                    wkt_field_header = in_col + '_' + OUT_SRID
                gdf_reproj[wkt_field_header] = wkt_reproj
                return gdf_reproj[[in_pk, wkt_field_header]]
            except Exception as e:
                self.logger.error(e.args[0])
                raise


        try:
            self.logger.info("starting add_stateplane_wkt()")
            if self.responseClass == 'household':
                self.logger.info("final_home_wgs84")
                rfdf['final_home_wgs84'] = 'Point(' + rfdf['final_home_lng'].astype(str) + ' ' + rfdf['final_home_lat'].astype(str) + ')'
                self.logger.info("prev_home_wgs84")
                rfdf['prev_home_wgs84'] = 'Point(' + rfdf['prev_home_lng'].astype(str) + ' ' + rfdf['prev_home_lat'].astype(str) + ')'
                self.logger.info("df_final_home")
                df_final_home = reproject_column_wgs84_to_stateplane(rfdf, 'hhid', 'final_home_wgs84')
                self.logger.info("df_prev_home")
                df_prev_home = reproject_column_wgs84_to_stateplane(rfdf, 'hhid', 'prev_home_wgs84')
                self.logger.info("rfdf1")
                rfdf = pd.merge(rfdf, df_final_home, on='hhid')
                self.logger.info("rfdf2")
                rfdf = pd.merge(rfdf, df_prev_home, on='hhid')
            elif self.responseClass == 'person':
                rfdf['school_wgs84'] = 'Point(' + rfdf['school_loc_lng'].astype(str) + ' ' + rfdf['school_loc_lat'].astype(str) + ')'
                rfdf['work_wgs84'] = 'Point(' + rfdf['work_lng'].astype(str) + ' ' + rfdf['work_lat'].astype(str) + ')'
                rfdf['prev_work_wgs84'] = 'Point(' + rfdf['prev_work_lng'].astype(str) + ' ' + rfdf['prev_work_lat'].astype(str) + ')'
            elif self.responseClass == 'trip':
                rfdf['origin_wgs84'] = 'Point(' + rfdf['origin_lng'].astype(str) + ' ' + rfdf['origin_lat'].astype(str) + ')'
                rfdf['dest_wgs84'] = 'Point(' + rfdf['dest_lng'].astype(str) + ' ' + rfdf['dest_lat'].astype(str) + ')'
            self.logger.info("finishing add_stateplane_wkt()")
            return rfdf

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def getResponseDF(self):
        return self.rfdf

    def getCodeBookDF(self):
        return self.cbdf


class load2019(load):

    def __init__(self, year, responseClass, responseFile, codeBookFile):
        super().__init__(year, responseClass, responseFile, codeBookFile)


    def StageCodeBookFile(self):

        try:
            self.logger.info("Starting staging codebook file")

            codebookHeaderRow = self.config.get('CodeBook',str(self.year)+self.responseClass+"header")
            codebookSheetName = self.config.get('CodeBook',str(self.year)+self.responseClass+"sheet")
            codebookDict = pd.read_excel(self.codeBookFile, index_col=None, header=int(codebookHeaderRow), sheet_name=codebookSheetName)
            codebookDF = pd.DataFrame.from_dict(codebookDict)

            self.logger.info("Starting transformation of codebook.")
            codebookDF = self.AddCodesForMissingData(codebookDF)
            codebookDF = codebookDF.drop_duplicates()
            self.cbdf = codebookDF

            self.logger.info("Starting insertion of codebook file data into staging table.")
            with SurveyDatabase.surveyDatabase() as db:
                db.createStagingTableFromDF(self.cbdf,'codebook_'+self.responseClass+str(self.year))

            self.logger.info("Finished insertion of codebook file data into staging table.")

        except Exception as e:
            self.logger.error(e.args[0])
            raise

    def AddCodesForMissingData(self, codebookDF):

        try:
            self.logger.info("Starting Staging.load2019.AddCodesForMissingData.")
            df_missing_data = codebookDF[codebookDF.variable == '_All categorical variables_'][['value', 'label']]
            df_alternate_missing_data = pd.DataFrame({'value': [-995], 'label':['Missing: Skip logic']}) # some data was mistakenly coded -995 instead of 995
            df_missing_data = df_missing_data.append(df_alternate_missing_data)
            df_main = codebookDF[codebookDF.variable != '_All categorical variables_']
            variables_list = df_main.variable.unique()
            df_variables = pd.DataFrame({'variable':variables_list})

            # 995 is a valid value for arrival_time_mam and depart_time_mam, so don't add skip logic lookup value for them.
            df_variables = df_variables[df_variables.variable != 'arrival_time_mam']
            df_variables = df_variables[df_variables.variable != 'depart_time_mam']

            combolist = list(product(df_variables.variable, df_missing_data.value))
            df_combinations = pd.DataFrame(data=combolist, columns=['variable','value'])
            df_full_emptydataset = pd.merge(df_combinations, df_missing_data, how='inner', on='value' )
            df_fullset = pd.concat([df_full_emptydataset,df_main])
            df_fullset = df_fullset.rename(columns={'variable':'Field','value':'Variable','label':'Value'})
            self.logger.info("Finishing Staging.load2019.AddCodesForMissingData.")
            return df_fullset

        except Exception as e:
            self.logger.error(e.args[0])
            raise


    def StageResponseFile(self):
        try:
            self.logger.info("Starting staging response file")
            with SurveyDatabase.surveyDatabase() as db:

                #read in where the header row starts
                header_row = int(self.config.get('Response',str(self.year)+self.responseClass+"header"))
                self.logger.info("Setting header row to: " + str(header_row))
                sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                self.logger.info("Setting sheet name to: " + sheet_name)

                self.logger.info("Reading in file format for " + str(self.year))
                readFormat = self.config.get('Response',str(self.year)+self.responseClass+"format")

                if readFormat == 'excel':
                    response_sheet_name = self.config.get('Response', str(self.year)+self.responseClass+"sheet")
                    self.logger.info("Setting sheet name to: " + response_sheet_name)
                    self.logger.info('Reading in survey excel file from: ' + self.responseFile)
                    rfdf = pd.read_excel(self.responseFile,
                            index_col=None,
                            header=header_row,
                            sheet_name=response_sheet_name)
                elif readFormat == 'database':
                    self.logger.info('Reading in survey from database table')
                    rfdf = db.executeAndPandas("SELECT * FROM [stg].["+self.responseFile+"]")
                elif readFormat == 'csv':
                    self.logger.info('Reading in survey csv file from: ' + self.responseFile)
                    rfdf = pd.read_csv(self.responseFile,
                        index_col=None,
                        header=header_row,
                        encoding='ISO-8859-1')
                    self.logger.info('finished reading survey file')
                else:
                    raise Exception('Unknown Format Type')

                rfdf = self.add_stateplane_wkt(rfdf)

                self.logger.info("Starting insertion of response file data into staging table.")
                db.createStagingTableFromDF(rfdf,'Survey_file_'+str(self.year))
                self.logger.info("Finished insertion of response file data into staging table.")

                self.rfdf = rfdf
        except Exception as e:
            self.logger.error(e.args[0])
            raise
