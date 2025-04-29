import findspark, uuid, os, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, length, trim, lit
import firmbackbone as fbb
import datetime
from colorama import Fore, Back
import re

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
print(COLOUR_HEAD + '  20241119 - KVK import  '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Start: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

########################################################################
### Configure environment
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading configuration...'.ljust(t_width) + COLOUR_REST)

# Set our configuration properties
file_id = str(uuid.uuid4())

# For the first KVK file for which we make a new backbone (2024 version since I believe the 2023 version is incorrect)
# source_location = 'C:/Users/<user>/Documents/FirmBackbone/fbb-data/20231221 - KvK/BASIS/'
# source_file = 'BASIS.csv'
# # source_file = 'basis_sample_extra.csv'
# source_wave = '2023-12-21'
# source_provider = 'kvk'
# target_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
# target_file = '20231221_kvk'
# create_backbone = True
# backbone_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
# backbone_file = 'backbone'
# source_backup = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'

# For the second KVK file (older one for testing purposes) for which we merge with the backbone
source_location = 'C:/Users/<user>/Documents/FirmBackbone/fbb-data/20240205 - KvK/'
source_file = 'BASIS.csv'
source_wave = '2024-02-05'
source_provider = 'kvk'
target_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
target_file = '20240205_kvk'
create_backbone = False
backbone_location = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'
# backbone_file = 'backbone_20231221'
backbone_file = 'backbone_20231221'
source_backup = 'C:/Users/<user>/Documents/FirmBackbone/pySpark_ETL/'

########################################################################
### Create a copy of the source file in bronze
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Backup source file...'.ljust(t_width) + COLOUR_REST)
os.makedirs(os.path.dirname(source_backup + 'bronze/'), exist_ok=True)
shutil.copyfile(source_location + source_file, 
                source_backup + 'bronze/' + f'{file_id}_{source_file}')
with open(source_location + f'{file_id}_{source_file}.txt', "w") as source_log:
    source_log.write(f'Timestamp: {datetime.datetime.now()}\n')
    source_log.write(f'Processed: {source_file}\n')
    source_log.write(f'Processor: 20241119 - KVK import\n')
    pass

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Initialize Spark...'.ljust(t_width) + COLOUR_REST)
findspark.init('c:/spark')
spark = SparkSession.builder \
    .appName('kvk_import_script') \
    .config('spark.driver.memory', '60g') \
    .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
    .config('spark.worker.cleanup.enabled', 'true') \
    .getOrCreate()    
    
        
########################################################################
### Load data into memory
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading data from CSV...'.ljust(t_width) + COLOUR_REST)
options = {
    'inferSchema': 'True',
    'delimiter': ';',
    'header': 'True',
    'quote': '"',
    'escape': '"'
}
src = spark.read.options(**options).csv(source_backup + f'bronze/{file_id}_{source_file}')

print(COLOUR_HEAD + f'!!! Counted rows after initial loading: {src.count()}'.ljust(t_width) + COLOUR_REST) 

########################################################################
### Process source data
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Transforming data structure...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Rename data columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_rename(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting DATE data columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_dates(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting BOOL data columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_boolean(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting INT data columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_integer(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Trimming STR columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_trim(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Creating unicode search columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_col_cleanup(src)

# Specific KVK related transformation
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Implementing KVK specific requirements...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping 0s from the annualy report years...'.ljust(t_width) + COLOUR_REST)
src = src.withColumn('annual_report_year', when(col('annual_report_year') == 0, None).otherwise(col('annual_report_year')))
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting country codes to ISO3c...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_create_iso3c(src)

# Specific FBB related transformations
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Implementing FBB strcutural requirements...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Creating abbreviated postcode columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_create_postcodes(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping exact duplicate rows...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_drop_duplicates(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Initializing standard backbone columns...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_create_fbb_cols(src, wave_date=source_wave, overwrite=True, file_id=file_id, data_source=source_provider)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Calculating column for latest change date...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_create_last_update(src)
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Calculating column for combined address...'.ljust(t_width) + COLOUR_REST)
src = fbb.df_create_combined_address(src)
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Load data changes in cache...'.ljust(t_width) + COLOUR_REST)
src.cache()

########################################################################
### Generate or retrieve backbone data
########################################################################

# Save the data as the initial backbone or open the initial backbone
if create_backbone:
    print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Generating new backbone...'.ljust(t_width) + COLOUR_REST)
    bb = src
    
    # Select only columns that should be in the backbone...
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Subsetting backbone columns...'.ljust(t_width) + COLOUR_REST)
    bb_cols = ['fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id', 'fbb_wave_id', 'fbb_updated', 'fbb_deleted', 'fbb_max_date', 'fbb_source', 'fbb_combined_address',
            'case_id', 'establishment_id', 'hq_indicator', 'company_name', 'establishment_name', 'partnership_name', 'code_legal_form', 'description_legal_form', 
            'establishment_dissolved', 'company_dissolved', 'code_status_legal', 
            'establishment_street', 'establishment_number', 'establishment_extension', 'establishment_postcode', 'establishment_pc4', 'establishment_city', 'establishment_code_municipality', 
            'correspondence_street', 'correspondence_number', 'correspondence_extension', 'correspondence_postcode', 'correspondence_pc4', 'correspondence_city', 'correspondence_code_municipality', 'correspondence_code_country', 'correspondence_code_iso3c',
            'kvk_city', 'employees_fulltime', 'employees_parttime', 'employees_total', 'date_employees', 'employment_total', 'date_employment', 
            'code_sbi_1', 'code_sbi_2', 'code_sbi_3', 'economically_active', 
            'registered_corporate_structure', 'indication_import', 'indication_export', 'date_founded', 'date_on_deed', 'date_registration', 'code_registration', 'date_cancellation', 'code_cancellation', 'code_deregistration', 'code_dissolution', 
            'date_establishment_reported', 'date_initial_establishment', 'date_current_establishment', 'date_continuation', 
            'date_request_suspension', 'date_active_suspension', 'code_suspension', 
            'date_request_bankruptcy', 'date_active_bankruptcy', 'code_bankruptcy', 
            'date_provisioning_report', 'annual_report_year', 'annual_report_type',
            'uni_company_name', 'uni_establishment_name', 'uni_partnership_name', 
            'uni_establishment_street', 'uni_establishment_number', 'uni_establishment_city',
            'uni_correspondence_street', 'uni_correspondence_number', 'uni_correspondence_city'
            ]
    for c in bb_cols:
        if c not in bb.columns:
            print(f'{datetime.datetime.now()} :: Column {c} is missing in the backbone data. Cancel operation.')
            exit()
    bb = bb.select(bb_cols)
    
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Generating identifiers...'.ljust(t_width) + COLOUR_REST)
    bb = fbb.df_generate_fbb_ids(bb)    
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Caching data to ensure consistency on UUIDs...'.ljust(t_width) + COLOUR_REST)
    bb.cache() 
    
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Checking for errors in the backbone data...'.ljust(t_width) + COLOUR_REST)
    bb, er = fbb.df_clear_bb_duplicates(bb)
    if er != None:
        print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing erroneuous backbone data...'.ljust(t_width) + COLOUR_REST)
        er.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave) + '_errors')
    if bb != None:
        print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing new backbone data...'.ljust(t_width) + COLOUR_REST)
        bb.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave))
else:
    print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading existing backbone data...'.ljust(t_width) + COLOUR_REST)
    bb = spark.read.parquet(backbone_location + backbone_file)


########################################################################
### Combine the src and bb files
########################################################################

# Now that we have the backbone data we can match the records. In the 
# source data we have generated blank placeholders for the fbb_id, 
# fbb_grp_id and fbb_est_id, so we need to match them. Possibly we could 
# enforce overwriting these values with the ones in the backbone if the 
# source file somehow already has these...

# We need to do some record matching with the original backbone
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Matching with backbone identifiers...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Exact matching on case_id and establishment_id...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'case_id' : 'case_id',
    'establishment_id' : 'establishment_id'
    }   
src = fbb.df_exact_match(x = src, y = bb, 
                         mapping = map_cols, 
                         override_matches = True,
                         allow_nulls = False)


# Records that do not have an fbb_id assigned based on the case_id and
# establishment_id matching, are either not correctly matched, do not
# have an establishment_id assigned, or are not registered in the backbone. 
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Exact matching on case_id, fbb_combined_address, code_sbi_1 and uni_company_name...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'case_id' : 'case_id',
    'fbb_combined_address' : 'fbb_combined_address',
    'code_sbi_1' : 'code_sbi_1',
    'uni_company_name' : 'uni_company_name'
    }   
src = fbb.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = True,
                         override_matches = False)

# Now that we have matched the data as good as possible, we need to update
# the backbone with changed and new information.

if not create_backbone:
    print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Updating existing backbone...'.ljust(t_width) + COLOUR_REST)

    # Now we want to generate new fbb_id's for all the unmatched data which
    # will then be added to the backbone (if the required columns are available).
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Retrieving backbone identifiers or generating new ones...'.ljust(t_width) + COLOUR_REST)
    src = fbb.df_append_fbb_ids(src, bb)
    src.cache()

    # Now that we have the full list with all possible ID's from either
    # the backbone data or newly generated uuid's, we will merge the two
    # files. This process will generate a lot of duplicates that will be
    # removed later.
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Merging the backbone with the newly added data...'.ljust(t_width) + COLOUR_REST)
    new_backbone = fbb.df_update_backbone(src, bb)
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping duplicates...'.ljust(t_width) + COLOUR_REST)
    new_backbone = fbb.df_drop_duplicates(new_backbone)
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Checking for errors in the backbone data...'.ljust(t_width) + COLOUR_REST)
    bb, er = fbb.df_clear_bb_duplicates(new_backbone)
    
    if er != None:
        print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing erroneuous backbone data...'.ljust(t_width) + COLOUR_REST)
        er.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave) + '_errors' )
    
    if bb != None:
        print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing new backbone data...'.ljust(t_width) + COLOUR_REST)
        bb.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave))

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Storing silver dataset...'.ljust(t_width) + COLOUR_REST)

print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Writing separate dataset for unmatched records...'.ljust(t_width) + COLOUR_REST)
unm = src.filter(src.fbb_id.isNull())
unm.repartition(1).write.mode('overwrite').parquet(target_location + 'silver/' + target_file + '_unmatched')

print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Writing separate dataset for matched records...'.ljust(t_width) + COLOUR_REST)
src = src.filter(src.fbb_id.isNotNull())
src.repartition(1).write.mode('overwrite').parquet(target_location + 'silver/' + target_file + '_full')

print(COLOUR_HEAD + f'!!! Written rows to silver data: {src.count()}'.ljust(t_width) + COLOUR_REST) 

########################################################################
### Stop the Spark session
########################################################################

spark.catalog.clearCache()
spark.stop()

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  20241119 - KVK import completed! '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Finish: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

# According to the KVK (https://www.kvk.nl/download/de_nummers_van_het_handelsregister_tcm109-365707.pdf, Page 13)
# Every event in which another person becomes owner of an entire company, 
# results in a new KVK number. This is because any legal issue related to 
# the company are not transferred to the new owner but remain under the 
# legal responsibility of the old owner. Providing a new KVK number 
# therefore, indicates that a new owner is in power.
#
# There are many ways in which the KVK number does not change:
# When the owner does not change, but for example only the legal form,
# no change in KVK number is needed. When an existing concern starts a
# new company, the active KVK number is used. When the company is 
# cancelled but remains legally available.
#
# The establishment (physical location and SBI code) number changes only
# when BOTH the location AND the activity code are changed at once. So
# when an establishement moves, no change of code and when it changes
# some business activities, no change of code is needed, but if both,
# then a new establishment code is created.
