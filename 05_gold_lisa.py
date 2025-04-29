# Now we need to generate gold LISA data, based on the gold KVK data.
# So we only want matched LISA data that is representative to the
# associated gold KVK data (so the right matched set, e.g. onlyh legal
# persons with a for-profit character).

# Process: We will take the fbb_id's from a gold KVK file, lookup the
# associated in the silver LISA files, perform the column selection and
# save it as gold LISA file.

import findspark, uuid, os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
from colorama import Fore, Back
import lisa as lisa

########################################################################
### Basic start up
########################################################################

os.system('cls')

(t_width, t_height) = os.get_terminal_size()

COLOUR_HEAD = Fore.WHITE + Back.RED
COLOUR_TASK = Fore.BLUE + Back.WHITE
COLOUR_STEP = Fore.WHITE + Back.LIGHTBLUE_EX
COLOUR_CONF = Fore.GREEN + Back.LIGHTYELLOW_EX
COLOUR_REST = Fore.RESET + Back.RESET

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  Utrecht University    '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '  FIRMBACKBONE v3.00    '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '  20250127: LISA Gold Producer '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Start: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

########################################################################
### Configure environment
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading configuration...'.ljust(t_width) + COLOUR_REST)

# Set our configuration properties
file_id = str(uuid.uuid4())

# For the first KVK file for which we make a new backbone (2024 version since I believe the 2023 version is incorrect)
source_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
source_file = '20241204_lisa_2022'
backbone_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
backbone_file = 'backbone_20231221_20240205'

print(COLOUR_TASK + f'>>> Initialize Spark...'.ljust(t_width) + COLOUR_REST)
findspark.init('c:/spark')
spark = SparkSession.builder \
    .appName('kvk_import_script') \
    .config('spark.driver.memory', '16g') \
    .config('spark.executor.memory', '32g') \
    .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
    .config('spark.worker.cleanup.enabled', 'true') \
    .getOrCreate()

########################################################################
### Load GOLD kvk data into memory
########################################################################

print(COLOUR_TASK + f'>>> Loading GOLD kvk data...'.ljust(t_width) + COLOUR_REST)
bb = spark.read.parquet(backbone_location + backbone_file)
bb = bb.select('fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id', 
               'fbb_wave_id', 'fbb_updated', 'fbb_deleted', 'hq_indicator', 'code_legal_form')

########################################################################
### Load SILVER source data into memory
########################################################################

print(COLOUR_TASK + f'>>> Loading silver data...'.ljust(t_width) + COLOUR_REST)
src = spark.read.parquet(source_location + 'silver/' + source_file + '_matched')

########################################################################
### Filter gold LISA and write to disk
########################################################################

__lisa_gold_cols = ['fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id', 'fbb_wave_id', 'fbb_updated', 'fbb_deleted', 'fbb_source', 
                    'sbi_description', 'sbi_sector_int', 'sbi_sector_str', 'code_sbi_1', 'code_sbi_2', 'code_sbi_3', 'code_sbi_4', 
                    'establishment_size_category', 'number_of_jobs', 'number_of_establishments', 'employees_fulltime', 'employees_parttime', 
                    'employees_female', 'employees_female_fulltime', 'employees_female_parttime', 
                    'employees_male', 'employees_male_fulltime', 'employees_male_parttime', 
                    'bag_object_surface', 'bag_object_usage', 'establishment_pc4', 'establishment_city', 
                    'establishment_code_corop', 'establishment_code_labor_region', 'establishment_code_lisa_region', 'establishment_code_municipality', 'establishment_code_province', 
                    'establishment_name_corop', 'establishment_name_labor_region', 'establishment_name_lisa_region', 'establishment_name_municipality', 'establishment_name_province', 
                    'counts_raised', 'lisa_survey_type', 'survey_content_year', 'survey_form_received', 'survey_registration', 'month_lisa_processed', 'year_lisa_processed',
                    'month_cancelled', 'year_cancelled', 'month_founded', 'year_founded']

def df_lisa_gold(x, y, ents, cols = __lisa_gold_cols):
    """Link the source with backbone subset selection.
    
    Description: Links the provided source data (x) with the backbone (y)
        and subsets only those records that have the appropriate legal
        entity types that are provided (ents). The subset column is
        not configurable since we are working with the standardized
        backbone file. The columns to export into the gold data are 
        configurable (cols).
    
    Args:
        x (Spark Dataframe): Source data that must be linked and filtered
        y (Spark Dataframe): Backbone information used for subsetting
        ents (List): List of legal type codes to subset on
        cols (List): List of columns (in y) that will be filtered.
    Returns:
        Spark Dataframe: Subsetted and filtered gold dataframe
    """
    for c in cols:
        if c not in x.columns:
            print(f'    {datetime.datetime.now()} !! Column {c} is not part of the specified source data.')
            raise Exception(f'!! Column {c} is not part of the specified source data.')
    if 'fbb_id' not in x.columns:
        print(f'    {datetime.datetime.now()} !! No FIRMBACKBPONE identifier known in the specified source data.')
        raise Exception(f'!! No FIRMBACKBPONE identifier known in the specified source data.')
    if 'fbb_id' not in y.columns:
        print(f'    {datetime.datetime.now()} !! No FIRMBACKBPONE identifier known in the specified backbone data.')
        raise Exception(f'!! No FIRMBACKBPONE identifier known in the specified backbone data.')
    if 'code_legal_form' not in y.columns:
        print(f'    {datetime.datetime.now()} !! No legal form known in the specified backbone data.')
        raise Exception(f'!! No legal form known in the specified backbone data.')

    y = y.filter(y.code_legal_form.isin(ents)).select('fbb_id')
    x = x.join(y, 'fbb_id', 'inner')
    x = x.select(cols)
    return x

print(COLOUR_STEP + f'    Storing full gold data...'.ljust(t_width) + COLOUR_REST)
src.select(__lisa_gold_cols).repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_complete')

print(COLOUR_STEP + f'    Storing gold non-natural legal persons data...'.ljust(t_width) + COLOUR_REST)
lisa_legal = df_lisa_gold(src, bb, ["40", "41", "42", "51", "52", "53", "55", "61", "62", "65", "70", "71", "72", "73", "74", "81", "82", "88", "89"])
lisa_legal.repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_legal')

print(COLOUR_STEP + f'    Storing gold non-natural for-profit legal persons data...'.ljust(t_width) + COLOUR_REST)
lisa_legal_commercial = df_lisa_gold(src, bb, ["40", "41", "42", "51", "52", "53", "55", "61", "62", "65", "81", "82", "89"])
lisa_legal_commercial.repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_legal_profit')

print(COLOUR_STEP + f'    Storing gold non-natural non-profit legal persons data, excluding public legal persons...'.ljust(t_width) + COLOUR_REST)
lisa_legal_nonprofit = df_lisa_gold(src, bb, ["70", "71", "72", "73", "74"])
lisa_legal_nonprofit.repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_legal_nonprofit')

print(COLOUR_STEP + f'    Storing gold foreignly owned legal persons data...'.ljust(t_width) + COLOUR_REST)
lisa_foreign = df_lisa_gold(src, bb, ["92", "93", "94", "96"])
lisa_foreign.repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_foreign')

print(COLOUR_STEP + f'    Storing gold public legal persons data...'.ljust(t_width) + COLOUR_REST)
lisa_public = df_lisa_gold(src, bb, ["88"])
lisa_public.repartition(1).write.mode('overwrite').parquet(source_location + 'gold/' + source_file + '_public')

########################################################################
### Stop the Spark session
########################################################################

spark.catalog.clearCache()
spark.stop()

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  20250127: Completed LISA Gold producer '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
