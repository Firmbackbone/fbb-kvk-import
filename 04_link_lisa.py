import findspark, uuid, os, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, length, trim, lit
from pyspark.sql.types import *
import pyspark.pandas as ps
import pandas as pd
import pyreadstat
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
print(COLOUR_HEAD + '  20250121 - LISA Linker '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Start: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

########################################################################
### Configure environment
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading configuration...'.ljust(t_width) + COLOUR_REST)

# Set our configuration properties
file_id = str(uuid.uuid4())

# For the first KVK file for which we make a new backbone (2024 version since I believe the 2023 version is incorrect)
source_backup = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
source_location = 'C:/Users/5775620/Documents/FirmBackbone/fbb-data/LISA/'
source_file = 'LISA_2022.sav'       # 'LISA_2022_ibis.sav'
source_csv_file = 'LISA_2022.csv'   # 'LISA_2022_ibis.csv'
source_wave = '2024-12-04'          # ''2021-09-12' / '2024-12-03'
source_provider = 'lisa'
target_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
target_file = '20241204_lisa_2022'
backbone_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/'
backbone_file = 'backbone_20231221_20240205'

########################################################################
### Create a copy of the source file in bronze
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Adjusting timezone settings (required for Spark + Pandas)...'.ljust(t_width) + COLOUR_REST)
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Backup source file...'.ljust(t_width) + COLOUR_REST)
os.makedirs(os.path.dirname(source_backup + 'bronze/'), exist_ok=True)
shutil.copyfile(source_location + source_file, 
                source_backup + 'bronze/' + f'{file_id}_{source_file}')
with open(source_location + f'{file_id}_{source_file}.txt', "w") as source_log:
    source_log.write(f'Timestamp: {datetime.datetime.now()}\n')
    source_log.write(f'Processed: {source_file}\n')
    source_log.write(f'Processor: 20250121 - LISA Linker\n')
    pass

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Initialize Spark...'.ljust(t_width) + COLOUR_REST)
findspark.init('c:/spark')
spark = SparkSession.builder \
    .appName('lisa_linking_script') \
    .config('spark.driver.memory', '60g') \
    .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
    .config('spark.worker.cleanup.enabled', 'true') \
    .getOrCreate()    
    
        
########################################################################
### Load data into memory
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Convert SAV file into CSV file...'.ljust(t_width) + COLOUR_REST)

# chunksize determines how many rows to be read per chunk
reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sav, 
                                        f'{source_backup}bronze/{file_id}_{source_file}', 
                                        chunksize= 100000)
cnt = 0
for df, meta in reader:
    # if on the first iteration write otherwise append
    if cnt > 0:
        wmode = "a"
        header = False
    else:
        wmode = "w"
        header = True
    # write
    print(f'    {datetime.datetime.now()} :: Writing data partition {cnt} to CSV...')
    df.to_csv(f'{source_backup}bronze/{file_id}_{source_csv_file}', 
              mode = wmode, header = header, index = False)
    cnt += 1

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Load CSV file into Apache Spark...'.ljust(t_width) + COLOUR_REST)
options = {
    'inferSchema': 'True',
    'delimiter': ',',
    'header': 'True',
    'quote': '"',
    'escape': '"'
}
src = spark.read.options(**options).csv(f'{source_backup}bronze/{file_id}_{source_csv_file}')

########################################################################
### Process source data
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Transforming data structure...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Rename data columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_rename(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping specific columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_remove(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting DATE data columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_dates(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting BOOL data columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_boolean(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Converting INT data columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_integer(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Trimming STR columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_trim(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Creating unicode search columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_cleanup(src)

# Specific LISA related transformation
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Implementing LISA specific requirements...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Ensuring presence of mandatory columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_ensure_columns(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Fix dropped leading 0s on case and establishment identifiers in LISA ...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_col_valid_kvk(src)

# Specific FBB related transformations
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Implementing FBB strcutural requirements...'.ljust(t_width) + COLOUR_REST)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Creating abbreviated postcode columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_create_postcodes(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping exact duplicate rows...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_drop_duplicates(src)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Initializing standard backbone columns...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_create_fbb_cols(src, wave_date=source_wave, overwrite=True, file_id=file_id, data_source=source_provider)
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Calculating column for latest change date...'.ljust(t_width) + COLOUR_REST)
# It's kind of useless to compare the last date from the foundation and cancellation date (always cancellation)
# Also it is not usefull to compare date_provided with date_processed (always processed)
# Finally, it is not usefull to compare date_cancelled (or founded) with processed, since this must always be processed too.
# But for compliance reasons, we keep this as not to alter the output structure...
src = lisa.df_create_last_update(src)
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Calculating column for combined address...'.ljust(t_width) + COLOUR_REST)
src = lisa.df_create_combined_address(src)
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Load data changes in cache...'.ljust(t_width) + COLOUR_REST)
src.cache()

########################################################################
### Generate or retrieve backbone data
########################################################################

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
# First matching on case_id and establishment_id would do the trick fully, 
# there should not be any changes in these identifiers over time, so 
# filtering is not needed, we can just pick the first record and link.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 01/08 - Exact matching on case_id and establishment_id...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'case_id' : 'case_id',
    'establishment_id' : 'establishment_id'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols, 
                         override_matches = True,
                         allow_nulls = False)

# Records that do not have an fbb_id assigned based on the case_id and
# establishment_id matching, may not have an establishment_id assigned, 
# or are not registered in the backbone. So let's check on the broadest 
# amount of reference points, address, name, sbi and case_id. We do not 
# allow for NULLs since we will relax the filtering conditions over time.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 02/08 - Exact matching on case_id, fbb_combined_address, code_sbi_1 and uni_establishment_name...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'case_id' : 'case_id',
    'fbb_combined_address' : 'fbb_combined_address',
    'code_sbi_1' : 'code_sbi_1',
    'uni_establishment_name' : 'uni_establishment_name'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         prefer_hq=True)

# Relax the matching assumptions to only look at address, sbi code, 
# establishment name and date founded, since there are files that do not 
# have a registered case_id in LISA.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 03/08 - Exact matching on fbb_combined_address, code_sbi_1 and uni_establishment_name, date_founded...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'fbb_combined_address' : 'fbb_combined_address',
    'code_sbi_1' : 'code_sbi_1',
    'uni_establishment_name' : 'uni_establishment_name',
    'date_founded' : 'date_founded'
    }
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         prefer_hq=True)

# Relax the matching assumptions to only look at address, sbi code 
# and establishment name, since there are files that do not 
# have a known case_id in LISA.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 04/08 - Exact matching on fbb_combined_address, code_sbi_1 and uni_establishment_name...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'fbb_combined_address' : 'fbb_combined_address',
    'code_sbi_1' : 'code_sbi_1',
    'uni_establishment_name' : 'uni_establishment_name'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         prefer_hq=True)

# Relax the matching assumptions to only look at address, sbi code 
# and establishment name, since there are files that do not 
# have a known case_id in LISA.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 05/08 - Exact matching on fbb_combined_address, uni_establishment_name and date_founded...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'fbb_combined_address' : 'fbb_combined_address',
    'uni_establishment_name' : 'uni_establishment_name',
    'date_founded' : 'date_founded'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         prefer_hq=True)

# Relax the matching assumptions to only look at establishment name and 
# foundation date.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 06/08 - Exact matching on uni_establishment_name and date_founded...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'uni_establishment_name' : 'uni_establishment_name',
    'date_founded' : 'date_founded'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         prefer_hq=True)

# Relax the matching assumptions even further, only look at company name, 
# assuming they still exist in the same city, BUT when multiple matches 
# occur, we cannot match.
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 07/08 - Exact matching on uni_establishment_city and uni_establishment_name...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'uni_establishment_city' : 'uni_establishment_city',
    'uni_establishment_name' : 'uni_establishment_name'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         force_unique=True,
                         prefer_hq=True)

# Last desperate step is to just look at the company names but ensure
# that we can only match when the match is unique!
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: 08/08 - Exact matching on uni_establishment_name...'.ljust(t_width) + COLOUR_REST)
map_cols = {
    'uni_establishment_name' : 'uni_establishment_name'
    }   
src = lisa.df_exact_match(x = src, y = bb, 
                         mapping = map_cols,
                         allow_nulls = False,
                         override_matches = False,
                         force_unique=True,
                         prefer_hq=True)


# if not create_backbone:
#     print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Updating existing backbone...'.ljust(t_width) + COLOUR_REST)

#     # Now we want to generate new fbb_id's for all the unmatched data which
#     # will then be added to the backbone (if the required columns are available).
#     print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Retrieving backbone identifiers or generating new ones...'.ljust(t_width) + COLOUR_REST)
#     src = fbb.df_append_fbb_ids(src, bb)
#     src.cache()

#     # Now that we have the full list with all possible ID's from either
#     # the backbone data or newly generated uuid's, we will merge the two
#     # files. This process will generate a lot of duplicates that will be
#     # removed later.
#     print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Merging the backbone with the newly added data...'.ljust(t_width) + COLOUR_REST)
#     new_backbone = fbb.df_update_backbone(src, bb)
#     print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Dropping duplicates...'.ljust(t_width) + COLOUR_REST)
#     new_backbone = fbb.df_drop_duplicates(new_backbone)
#     print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Checking for errors in the backbone data...'.ljust(t_width) + COLOUR_REST)
#     bb, er = fbb.df_clear_bb_duplicates(new_backbone)
    
#     if er != None:
#         print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing erroneuous backbone data...'.ljust(t_width) + COLOUR_REST)
#         er.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave) + '_errors' )
    
#     if bb != None:
#         print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Storing new backbone data...'.ljust(t_width) + COLOUR_REST)
#         bb.repartition(1).write.mode('overwrite').parquet(backbone_location + backbone_file + '_' + re.sub('-', '', source_wave))

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Storing silver dataset...'.ljust(t_width) + COLOUR_REST)

print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Writing separate dataset for unmatched records...'.ljust(t_width) + COLOUR_REST)
unm = src.filter((src.fbb_id.isNull()) | (src.fbb_id == ''))
if unm is not None:
    unm.repartition(1).write.mode('overwrite').parquet(target_location + 'silver/' + target_file + '_unmatched')
else:
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: No unmatched records found...'.ljust(t_width) + COLOUR_REST)

print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Writing separate dataset for matched records...'.ljust(t_width) + COLOUR_REST)
src = src.filter((src.fbb_id.isNotNull()) & (src.fbb_id != ''))

print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Writing matched dataset to disk...'.ljust(t_width) + COLOUR_REST)
if src is not None:
    src.repartition(1).write.mode('overwrite').parquet(target_location + 'silver/' + target_file + '_matched')
else:
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: No matched records found...'.ljust(t_width) + COLOUR_REST)

########################################################################
### Stop the Spark session
########################################################################

spark.catalog.clearCache()
spark.stop()

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  20250121 - LISA Linker completed! '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Finish: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
