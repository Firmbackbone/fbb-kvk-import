import findspark, uuid, os, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, length, trim, lit
import firmbackbone as fbb
from datetime import date
from colorama import Fore, Back
import pyspark
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

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
print(COLOUR_HEAD + '  20241209: Generate Gold Data  '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

########################################################################
### Configure environment
########################################################################

print(COLOUR_TASK + f'>>> Loading configuration...'.ljust(t_width) + COLOUR_REST)

# Set our configuration properties
file_id = str(uuid.uuid4())

# # For the first KVK file for which we make a new backbone (2024 version since I believe the 2023 version is incorrect)
# source_location = 'C:/Users/5775620/Documents/FirmBackbone/fbb-data/20231221 - KvK/BASIS/'
# source_file = 'BASIS.csv'
# source_wave = '2023-12-21'
# target_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
# target_file = '20231221_kvk'
# create_backbone = True
# backbone_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
# backbone_file = 'backbone_20231221'
# source_backup = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'

# For the second KVK file (older one for testing purposes) for which we merge with the backbone
source_location = 'C:/Users/5775620/Documents/FirmBackbone/fbb-data/20240205 - KvK/'
source_file = 'BASIS.csv'
source_wave = '2024-02-05'
target_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
target_file = '20240205_kvk'
create_backbone = False
backbone_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
backbone_file = 'backbone_20231221_20240205'
source_backup = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'

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
### Loading Silver data
########################################################################
print(COLOUR_TASK + f'>>> Loading silver data...'.ljust(t_width) + COLOUR_REST)
src = spark.read.parquet(target_location + '/silver/' + target_file + '_full')

# Finally we need to create the gold datasets
print(COLOUR_TASK + f'>>> Storing gold datasets...'.ljust(t_width) + COLOUR_REST)

gold_cols = ['fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id', 'fbb_wave_id', 'fbb_updated', 'fbb_deleted',
             'hq_indicator', 'code_legal_form', 'establishment_dissolved', 'company_dissolved', 'code_status_legal', 
             'establishment_pc4', 'establishment_city', 'establishment_code_municipality', 
             'correspondence_pc4', 'correspondence_city', 'correspondence_code_municipality', 'correspondence_code_country', 
             'employees_fulltime', 'employees_parttime', 'employees_total', 'year_employees', 'month_employees', 'employment_total', 
             'year_employment', 'month_employment', 'code_sbi_1', 'code_sbi_2', 'code_sbi_3', 'economically_active', 'indication_import', 'indication_export',
             'year_founded', 'month_founded', 'year_on_deed', 'month_on_deed', 'year_registration', 'month_registration', 
             'year_cancellation', 'month_cancellation', 'year_establishment_reported', 'month_establishment_reported', 
             'year_initial_establishment', 'month_initial_establishment', 'year_current_establishment', 'month_current_establishment', 
             'year_continuation', 'month_continuation', 'year_request_suspension', 'month_request_suspension', 'year_active_suspension', 'month_active_suspension', 
             'year_request_bankruptcy', 'month_request_bankruptcy', 'year_active_bankruptcy', 'month_active_bankruptcy', 'year_provisioning_report', 'month_provisioning_report'
             ]

print(COLOUR_STEP + f'    Storing full gold data...'.ljust(t_width) + COLOUR_REST)
kvk_gold = src.select(gold_cols)
kvk_gold.cache()
kvk_gold.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_complete')

# We filter out all legal forms which are their own legal person (privaatrechtelijke rechtsvorm)
# For these forms, the owners' assets can not be used to settle corporate debt. 
# We do not exclude the governmental legal form (publiekrechtelijke rechtsvorm, 88) and other underfined private legal forms (privaatrechtelijke rechtsvorm, 89).
# We exclude European economic collaborations (93) since they can not be resurrected since 2019 and should be replaced in 2024.
# Also other foreignly owned organizations that are registered in The Netherlands are excluded.
print(COLOUR_STEP + f'    Storing gold non-natural legal persons data...'.ljust(t_width) + COLOUR_REST)
# legal_filter_list = [40, 41, 42, 51, 52, 53, 55, 61, 62, 65, 70, 71, 72, 73, 74, 81, 82, 88, 89]
# legal_filter_df = spark.createDataFrame(legal_filter_list, StringType())
# legal_filter_df = legal_filter_df.withColumn('value', col('value').cast(IntegerType()))
# legal_filter_df = legal_filter_df.withColumnRenamed('value', 'code_legal_form')
# kvk_legal = kvk_gold.join(legal_filter_df, ['code_legal_form'], 'inner')
kvk_legal = kvk_gold.filter(kvk_gold.code_legal_form.isin(["40", "41", "42", "51", "52", "53", "55", "61", "62", "65", "70", "71", "72", "73", "74", "81", "82", "88", "89"]))
kvk_legal.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_legal')

# Now we filter out all legal entity types that do not have a profit focus (they could make a profit but the profit can not be paid out to the owners or memebers).
print(COLOUR_STEP + f'    Storing gold non-natural for-profit legal persons data...'.ljust(t_width) + COLOUR_REST)
# legal_filter_list = [40, 41, 42, 51, 52, 53, 55, 61, 62, 65, 81, 82, 88, 89]
# legal_filter_df = spark.createDataFrame(legal_filter_list, StringType())
# legal_filter_df = legal_filter_df.withColumn('value', col('value').cast(IntegerType()))
# legal_filter_df = legal_filter_df.withColumnRenamed('value', 'code_legal_form')
# kvk_legal_commercial = kvk_gold.join(legal_filter_df, ['code_legal_form'], 'inner')
kvk_legal_commercial = kvk_gold.filter(kvk_gold.code_legal_form.isin(["40", "41", "42", "51", "52", "53", "55", "61", "62", "65", "81", "82", "89"]))
kvk_legal_commercial.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_legal_profit')

# And all non-profit legal persons EXCLUDING public legal persons
print(COLOUR_STEP + f'    Storing gold non-natural non-profit legal persons data, excluding public legal persons...'.ljust(t_width) + COLOUR_REST)
# legal_filter_list = [70, 71, 72, 73, 74]
# legal_filter_df = spark.createDataFrame(legal_filter_list, StringType())
# legal_filter_df = legal_filter_df.withColumn('value', col('value').cast(IntegerType()))
# legal_filter_df = legal_filter_df.withColumnRenamed('value', 'code_legal_form')
# kvk_legal_nonprofit = kvk_gold.join(legal_filter_df, ['code_legal_form'], 'inner')
kvk_legal_nonprofit = kvk_gold.filter(kvk_gold.code_legal_form.isin(["70", "71", "72", "73", "74"]))
kvk_legal_nonprofit.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_legal_nonprofit')

# Look at foreign owned companies only, including the European economic collaborations.
print(COLOUR_STEP + f'    Storing gold foreignly owned legal persons data...'.ljust(t_width) + COLOUR_REST)
# legal_filter_list = [92, 93, 94, 96]
# legal_filter_df = spark.createDataFrame(legal_filter_list, StringType())
# legal_filter_df = legal_filter_df.withColumn('value', col('value').cast(IntegerType()))
# legal_filter_df = legal_filter_df.withColumnRenamed('value', 'code_legal_form')
# kvk_foreign = kvk_gold.join(legal_filter_df, ['code_legal_form'], 'inner')
kvk_foreign = kvk_gold.filter(kvk_gold.code_legal_form.isin(["92", "93", "94", "96"]))
kvk_foreign.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_foreign')

# Finally we filter on only public legal persons (i.e. state related organizations, perhaps this may NOT be exposed???)
print(COLOUR_STEP + f'    Storing gold public legal persons data...'.ljust(t_width) + COLOUR_REST)
# legal_filter_list = [88]
# legal_filter_df = spark.createDataFrame(legal_filter_list, StringType())
# legal_filter_df = legal_filter_df.withColumn('value', col('value').cast(IntegerType()))
# legal_filter_df = legal_filter_df.withColumnRenamed('value', 'code_legal_form')
# kvk_public = kvk_gold.join(legal_filter_df, ['code_legal_form'], 'inner')
kvk_public = kvk_gold.filter(kvk_gold.code_legal_form.isin(["88"]))
kvk_public.repartition(1).write.mode('overwrite').parquet(target_location + 'gold/' + target_file + '_public')

########################################################################
### Stop the Spark session
########################################################################

spark.catalog.clearCache()
spark.stop()

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  20241209: Gold data completed! '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
