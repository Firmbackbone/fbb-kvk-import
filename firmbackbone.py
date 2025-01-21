import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, BooleanType
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import regexp_extract, regexp_replace, row_number
from pyspark.sql.functions import when, coalesce, length, trim, concat, lit
from pyspark.sql.functions import lower, count, when, current_date, greatest
from pyspark.sql.functions import col, row_number, first, last, collect_set, size
from pyspark.sql.window import Window

import re
import uuid
import datetime as dt
import datetime


########################################################################
### Renaming Columns
########################################################################

__all_cols = {
    'Dossier' : 'case_id',
    'Vestigingsnummer' : 'establishment_id',
    'CdHoofdNevenvestiging' : 'hq_indicator',
    'Handelsnaam' :  'company_name',
    'Zaaknaam' :  'establishment_name',
    'Vennootschapsnaam' :  'partnership_name',
    'Cdrechtsvorm' :  'code_legal_form',
    'OmschrijvingRechtsvormfijn' :  'description_legal_form',
    'Cdstatusvestiging' :  'establishment_dissolved',
    'Cdstatusonderneming' :  'company_dissolved',
    'Statusrechtspersoon' :  'code_status_legal',
    'Codestatusdossier' :  'code_status_case',
    'StatusDossier' :  'text_status_case',
    'Straatvestadrs' :  'establishment_street',
    'Huisnrvestadrs' :  'establishment_number',
    'Toevoegingvestadrs' :  'establishment_extension',
    'Postcdvestadrs' :  'establishment_postcode',
    'Pltsvestadrs' :  'establishment_city',
    'Cdgemvestadrs' :  'establishment_code_municipality',
    'Straatcorradrs' :  'correspondence_street',
    'Huisnrcorradrs' :  'correspondence_number',
    'Toevoegingcorradrs' :  'correspondence_extension',
    'Postcdcorradrs' :  'correspondence_postcode',
    'Pltscorradrs' :  'correspondence_city',
    'Cdgemcorradrs' :  'correspondence_code_municipality',
    'Cdlndcorradrs' :  'correspondence_code_country',
    'Statzetel' :  'kvk_city',
    'Aantprswrkfulltime' :  'employees_fulltime',
    'Aantprswrkparttime' :  'employees_parttime',
    'Aantprswrktotaal' :  'employees_total',
    'Datpeilingprswrk' :  'date_employees',
    'WPTotaalonderneming' :  'employment_total',
    'PeildatumWPonderneming' :  'date_employment',
    'Cdhoofdactiviteit' :  'code_sbi_1',
    'Cdnevenactiviteit1' :  'code_sbi_2',
    'Cdnevenactiviteit2' :  'code_sbi_3',
    'Codeeconomischactief' :  'economically_active',
    'EconomischActief' :  'text_economically_active',
    'OndernemingsstructuurAanwezig' :  'registered_corporate_structure',
    'Indimport' :  'indication_import',
    'Indexport' :  'indication_export',
    'Datoprichting' :  'date_founded',
    'Datakteoprichting' :  'date_on_deed',
    'Datinschr' :  'date_registration',
    'Cdrdninschr' :  'code_registration',
    'Datopheffing' :  'date_cancellation',
    'Cdrdnopheffing' :  'code_cancellation',
    'Cdrdnuitschr' :  'code_deregistration',
    'Cdsrtontbinding' :  'code_dissolution',
    'Datvestvg' :  'date_establishment_reported',
    'datumvestiging' :  'date_initial_establishment',
    'DatVestHuidigVest' :  'date_current_establishment',
    'Datvoortzdooroo' :  'date_continuation',
    'Datingaanvsurs' :  'date_request_suspension',
    'Datingeindesurs' :  'date_active_suspension',
    'Cdsrteindesurs' :  'code_suspension',
    'Datingaanvfail' :  'date_request_bankruptcy',
    'Datingeindefail' :  'date_active_bankruptcy',
    'Cdsrteindefail' :  'code_bankruptcy',
    'CodeNonMailing' :  'code_non_mailing',
    'NonMailing' :  'text_non_mailing',
    'Datdepjaarstuk' :  'date_provisioning_report',
    'Boekjaar' :  'annual_report_year',
    'Cdsrtjaarstuk' :  'annual_report_type'
    }   

def df_col_rename(x, mapping: dict = __all_cols):
    """Automatically rename specified columns in a Spark dataframe. .
    
    Description:
        For the given Spark dataframe (x) a dictionary object is provided 
        (mapping) that maps any existing column name to a new column name. 
        When a column is provided that does not exist, the rename action
        is ignored.
        
    Args:
        x (Spakr DAtaframe): The Spark dataframe that contains the columns to be renamed.
        mapping (dict): Dictionary object containing the original and new name.
        
    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_rename()')
    for c in list(mapping.keys()):
        if c in x.columns:
            x = x.withColumnRenamed(c, mapping.get(c))
    return x

########################################################################
### Converting Column Types
########################################################################

__date_cols = {'date_employees' : 'yyyyMMdd', 
             'date_employment' : 'yyyyMMdd', 
             'date_founded' : 'yyyyMMdd', 
             'date_on_deed' : 'yyyyMMdd', 
             'date_registration' : 'yyyyMMdd', 
             'date_cancellation' : 'yyyyMMdd', 
             'date_establishment_reported' : 'yyyyMMdd', 
             'date_initial_establishment' : 'dd-MM-yyyy', 
             'date_current_establishment' : 'yyyyMMdd', 
             'date_continuation' : 'yyyyMMdd', 
             'date_request_suspension' : 'yyyyMMdd', 
             'date_active_suspension' : 'yyyyMMdd', 
             'date_request_bankruptcy' : 'yyyyMMdd', 
             'date_active_bankruptcy' : 'yyyyMMdd', 
             'date_provisioning_report' : 'yyyyMMdd'
             }

def df_col_dates(x, mapping: dict = __date_cols):
    """Convert and extends given date columns to date and integer cols.
    
    Description:
        After conversion the column is stored as a date object and three new
        columns are added to the dataframe with the prefixes, year_, month_ 
        and dayofmonnth_ which all contains the appropriate numerical value.
    
    Args:
        x (Spark Dataframe): The Spark dataframe that contains the columns to be converted.
        mapping (dict): Dictionary object containing the column name and used
        input formatt for the date.
        
    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_dates()')
    for c in list(mapping.keys()):
        if c in x.columns:
            x = x.withColumn(c, f.to_date(c, mapping.get(c)))
            x = x.withColumn(f'year_{c[5:]}', year(c))
            x = x.withColumn(f'month_{c[5:]}', month(c))
            x = x.withColumn(f'day_{c[5:]}', dayofmonth(c))
    return x

__bool_cols = {'establishment_dissolved' : '80 00', 
               'company_dissolved' : '80 00',
               'economically_active' : '1 0',
               'indication_import' : 'J N',
               'indication_export' : 'J N'
               }

def df_col_boolean(x, mapping: dict = __bool_cols, trueonly: bool = False):
    """Convert given list of columns into boolean type.
    
    Description:
        This code converts the predefined columns into boolean data type
        given the provided 
    
    Args:
        x (Spark Dataframe): The Spakr dataframe that contain the columns to be converted.
        mapping (dict): A list of column names that are to be replaced where the
        provided value is the 'true' value, all others become 'false'.
        trueonly (bool): By default this value is false, indicating that both 
        the true and false value must be specified sequentially with a space
        between the two values: 'true false' or '1 0'. When trueonly is set 
        to true, then the specification requires only the true value, all 
        other values incliding NA's are converted to 'false'.
        
    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_boolean()')
    for c in list(mapping.keys()):
        if c in x.columns:
            m = str(mapping.get(c))
            t_value = 'true'
            f_value = 'false'
            if trueonly:
                m_true = m 
                m_false = None               
            else:
                m = m.split(" ")
                m_true = m[0]
                m_false = m[1]
            if dict(x.dtypes)[c] == 'int': 
                m_true = int(m_true)
                if m_false != None:
                    m_false = int(m_false)
                t_value = 1
                f_value = 0
            if trueonly:
                x = x.withColumn(c, when(col(c) == m_true, t_value).otherwise(f_value))
            else:
                x = x.withColumn(c, when(col(c) == m_true, t_value) \
                    .when(col(c) == m_false, f_value).otherwise(None))
            x = x.withColumn(c, col(c).cast(BooleanType()))
    return x

__int_cols = ['code_legal_form', 'code_status_legal', 'employees_fulltime', 'employees_parttime', 'employees_total', 'employment_total']

def df_col_integer(x, cols: list = __int_cols):
    """Convert a given set of Spark dataframe columns to integer type.

    Description:
        Converts all the provided columns (if they exist) to integer format.
        
    Args:
        x (Spark Dataframe): Provides the Spark dataframe that contains
        the columns on which the transformation must be performed.
        cols (list): Provides a list of all column names that need to be
        converted.

    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_integer()')
    for c in cols:
        if c in x.columns:
            x = x.withColumn(c , x[c].cast(IntegerType()))
    return x

########################################################################
### Create Matching Columns - Clean Unwanted Text
########################################################################

__cleanup_cols = ['company_name', 'establishment_name', 'partnership_name',
                  'establishment_street', 'establishment_number', 'establishment_city',
                  'correspondence_street', 'correspondence_number', 'correspondence_city']

def df_col_cleanup(x, cols: list = __cleanup_cols):
    """Create cleaned unicode columns for matching.

    Description: 
        This function takes a Spark Dataframe as input (x) andapplies a 
        cleaning function to all provided known columns (cols). If a 
        column does not exist, it is skipped. The code removes corporate
        abbreviations in Dutch (B.V., N.V., v.o.f. etc.), removes all 
        whitespace and special characters and converts the text to lower 
        case. The cleaned up columns are stored with the prefix 'uni_*'.
    
    Args:
        x (Spark Dataframe): Contains the dataframe on which the operation is executed.
        cols (list, optional): Creates a list of all columns to be converted. Defaults to __cleanup_cols.

    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_cleanup()')
    for c in cols:
        if c in x.columns:
            x = x.withColumn(f'uni_{c}', lower(col(c)))
            x = x.withColumn(f'uni_{c}', regexp_replace(f'uni_{c}', 'b.v.|n.v.|v.o.f.|bv|nv|vof', ''))
            x = x.withColumn(f'uni_{c}', regexp_replace(f'uni_{c}', r'\s', ''))
            x = x.withColumn(f'uni_{c}', regexp_replace(f'uni_{c}', r'\W', ''))
            x = x.withColumn(f'uni_{c}', when(col(f'uni_{c}').isNull(), f.lit('')).otherwise(col(f'uni_{c}')))
    return x

__trim_cols = ['case_id', 'establishment_id',
               'company_name', 'establishment_name', 'partnership_name',
               'establishment_street', 'establishment_number', 'establishment_extension', 'establishment_city',
               'correspondence_street', 'correspondence_number', 'correspondence_extension', 'correspondence_city',
               'code_sbi_1', 'code_sbi_2', 'code_sbi_3'
               ]

def df_col_trim(x, cols: list = __trim_cols):
    """Trim columns to ensure no non-blank blanks.

    Description: 
        This function takes a Spark Dataframe as input (x) and trims all 
        the values in the provides columns (cols). Trimming means taking
        away all the leading and lagging spaces from a string value. The
        column values are overwritten. When an empty string remains, we
        convert this to the NULL or None value.
    
    Args:
        x (Spark Dataframe): Contains the dataframe on which the operation is executed.
        cols (list, optional): Creates a list of all columns to be converted. Defaults to __cleanup_cols.

    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_col_trim()')
    for c in cols:
        if c in x.columns:
            x = x.withColumn(c, trim(col(c)))
            x = x.withColumn(c, when(f.col(c).isNull(), f.lit('')).otherwise(f.col(c)))
            # While having NULL values is better (there is no value) it
            # causes problems with joining data since NULL != NULL, thus
            # we convert all NULL values to empty strings in these columns.
            # x = x.withColumn(c, when(col(c) != '', col(c)).otherwise(f.lit(None)))
            # x = x.na.fill('', [c])
    return x

def df_col_clean_nulls(x, cols: list = __trim_cols, replacement_value = ''):
    """Remove all NULL values form the dataset.
    
    Description:
        Whenever Python and Spark perform comparisons, by definition the
        values for NULL are different: NULL != NULL. Hence comparing rows
        with NULL values will therefore result in being non-duplicates.
        We need to remove all the NULL values and replace them for some
        other vlaue.
    
    Args:
        x (Spark Dataframe): Dataframe containing the source data.
        cols (List): List of column names that need to be replaced.
        replacement_value (String): The string that will replace NULLs

    Returns:
        Spark Dataframe, which is a copy of x with all NULLs replaced.
    """
    x = x.fillna(value = replacement_value, subset = cols)
    return x

########################################################################
### Create Specific FBB columns
########################################################################

def valid_uuid(str_uuid:str = None):
    """Check if a provided string is a valid uuid4().
    
    Args:
        str_uuid (str): String with the uuid4 format.
        
    Returns:
        Boolean: true if the uuid is valid.
    """
    print(f'    {datetime.datetime.now()} >> valid_uuid()')
    try:
        val = uuid.UUID(str_uuid, version=4)
    except ValueError:
        return False
    return str(val) == str_uuid

def valid_date(date_string: str = None):
    print(f'    {datetime.datetime.now()} >> valid_date()')
    try:
        dt.datetime.strptime(date_string, '%Y-%m-%d')   
        return True
    except ValueError:
        return False

__fbb_cols = [
    'fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id',
    'fbb_wave_id', 'fbb_updated', 'fbb_deleted', 'fbb_max_date', 'fbb_source',
    'fbb_combined_address'
    ]

def df_move_to_front(x, columns_to_front):
    """Moves selected columns to the first set of columns.
    
    Description:
        Sometimes it is nice to have specific columns first, this function
        sorts them according to the input specification and places them
        in front.
    
    Args:
        x (Spark Dataframe): The data that needs to have the columns moved.
    
    Returns:
        Spark Dataframe: Resulting dataframe with proper first columns.
    """
    print(f'    {datetime.datetime.now()} >> df_move_to_front()')
    original = x.columns
    # Filter to present columns
    columns_to_front = [c for c in columns_to_front if c in original]
    # Keep the rest of the columns and sort it for consistency
    columns_other = list(set(original) - set(columns_to_front))
    columns_other.sort()
    # Apply the order
    x = x.select(*columns_to_front, *columns_other)
    return x

def df_create_fbb_cols(x, wave_date = None, file_id:str = None, proc_date = None, data_source = 'undefined', overwrite:bool = False):
    """Creates the FBB standard columns and fills them if possible.
    
    Description:
        This procedure creates the firmbackbone specific columns that are 
        needed in all FBB gold, silver and backbone data. These columns 
        are: the fbb_id, fbb_grp_id, fbb_est_id, fbb_row_id, fbb_file_id, 
        fbb_wave_id, fbb_updated, fbb_deleted.
    
    Args:
        x (Spark Dataframe): Contains the dataframe on which the operation 
            is executed.
        wave_date (String): Contains the wave identifier in the format 
            yyyy-MM-dd (by default today). If incorrectly providded the date
            will be converted to 1900-01-01 and needs manual updating manually.
        file_id (String): Contains a unique UUID4 string (not hex) that 
            identifies the source file. If no uuid is provided or an incorrect 
            one, a new uuid4 is generated.
        proc_date (String): Contains the process date in the format 
            yyyy-MM-dd (by default today). If incorrectly providded the date
            will be converted to 1900-01-01 and needs manual updating manually.
        overwrite (Boolean): Defaults to False, when False new columns will be
            added only when they don't exist and not data in these columns will
            be changed (or validated). When set to True, new columns with are
            always created and the old ones are dropped.
    """
    print(f'    {datetime.datetime.now()} >> df_create_fbb_cols()')
    # Check if the provided file_id is a valid UUID
    if (file_id is None): 
        file_id = str(uuid.uuid4())
    else:
        if not valid_uuid(file_id):
            file_id = str(uuid.uuid4())
    # Check if the provided proc_date is a valid date
    if (proc_date is None): 
        proc_date = str(dt.date.today().strftime('%Y-%m-%d'))
    else:
        if not valid_date(proc_date):
            proc_date = '1900-01-01'
    # Check if the provided proc_date is a valid date
    if (wave_date is None): 
        wave_date = str(dt.date.today().strftime('%Y-%m-%d'))
    else:
        if not valid_date(wave_date):
            wave_date = '1900-01-01'
    
    # Create the columns
    if overwrite:
        x = x.withColumn('fbb_id', f.lit('').cast(StringType()))
        x = x.withColumn('fbb_grp_id', f.lit('').cast(StringType()))
        x = x.withColumn('fbb_est_id', f.lit('').cast(StringType()))
        x = x.withColumn('fbb_row_id', f.expr("uuid()"))
        x = x.withColumn('fbb_file_id', f.lit(file_id))
        x = x.withColumn('fbb_wave_id', f.lit(wave_date))
        x = x.withColumn('fbb_wave_id', f.to_date('fbb_wave_id', 'yyyy-MM-dd'))
        x = x.withColumn('fbb_updated', f.lit(proc_date))
        x = x.withColumn('fbb_updated', f.to_date('fbb_updated', 'yyyy-MM-dd'))
        x = x.withColumn('fbb_deleted', f.lit(None).cast(StringType()))
        x = x.withColumn('fbb_deleted', f.to_date('fbb_deleted', 'yyyy-MM-dd'))
        x = x.withColumn('fbb_source', f.lit(data_source))
        x = x.withColumn('fbb_source', f.lit(data_source))
    else:
        if 'fbb_id' not in x.columns: 
            x = x.withColumn('fbb_id', f.lit('').cast(StringType()))
        if 'fbb_grp_id' not in x.columns: 
            x = x.withColumn('fbb_grp_id', f.lit('').cast(StringType()))
        if 'fbb_est_id' not in x.columns: 
            x = x.withColumn('fbb_est_id', f.lit('').cast(StringType()))
        if 'fbb_row_id' not in x.columns: 
            x = x.withColumn('fbb_row_id', f.expr("uuid()"))
        if 'fbb_file_id' not in x.columns: 
            x = x.withColumn('fbb_file_id', f.lit(file_id))
        if 'fbb_wave_id' not in x.columns: 
            x = x.withColumn('fbb_wave_id', f.lit(wave_date))
            x = x.withColumn('fbb_wave_id', f.to_date('fbb_wave_id', 'yyyy-MM-dd'))
        if 'fbb_updated' not in x.columns: 
            x = x.withColumn('fbb_updated', f.lit(proc_date))
            x = x.withColumn('fbb_updated', f.to_date('fbb_updated', 'yyyy-MM-dd'))
        if 'fbb_deleted' not in x.columns: 
            x = x.withColumn('fbb_deleted', f.lit(None).cast(StringType()))
            x = x.withColumn('fbb_deleted', f.to_date('fbb_deleted', 'yyyy-MM-dd'))
        if 'fbb_source' not in x.columns: 
            x = x.withColumn('fbb_source', f.lit(data_source))
        if 'fbb_combined_address' not in x.columns: 
            x = x.withColumn('fbb_combined_address', f.lit('').cast(StringType()))
            
    x = df_move_to_front(x, __fbb_cols)
    return x

__postcode_cols = {
    'establishment_postcode': 'establishment_pc4',
    'correspondence_postcode': 'correspondence_pc4'
}

def df_create_postcodes(x, mapping:dict = __postcode_cols):
    print(f'    {datetime.datetime.now()} >> df_create_postcodes()')
    for c in mapping.keys():
        if c in x.columns:
            x = x.withColumn(mapping.get(c), regexp_extract(col(c), r'\d+', 0))
    return x

__compare_date_cols = ['date_employees', 'date_employment','date_founded', 
                       'date_on_deed', 'date_registration', 'date_cancellation',
                       'date_establishment_reported', 'date_initial_establishment',
                       'date_current_establishment', 'date_continuation', 
                       'date_request_suspension', 'date_active_suspension',
                       'date_request_bankruptcy','date_active_bankruptcy'
                       ]

def df_create_last_update(x, colDate = 'fbb_max_date', cols = __compare_date_cols, maxDate = True, replaceDateCol = True):
    """Get the maximum or minumum date from all specified columns per row.
    
    Description:
        This fucntion gets the maximum or minimim date that appears in 
        the provided columns and NULL values are ignored.
        
    Args:
        x (Spark Dataframe): Spark dataframe containing dates to be used.
        colDate (String): Column name for (new) column with min/max date.
        cols (List): List of columns to include in row-based comparison.
        maxDate (Boolean): Select maximum date (true, default), otherwise minimum.
        replaceDateCol (Boolean): Create/replace colDate (true, default)
            otherwise update colDate when value is blank.
        
    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_create_last_update()')
    for c in cols:
        if c not in x.columns:
            raise Exception(f'!! Column {c} is not found in the source data (x).')
    if (colDate not in x.columns) & (not replaceDateCol):
        raise Exception(f'!! Date column {colDate} is not found in the source data (x) and it cannot be created (replaceDateCol = False).')

    # Write code here to get the max or min date of all provided columns
    # and write this one to the newly created or already existing column.
    if not replaceDateCol:
        x = x.withColumn(colDate, when(col(colDate).isNull(), greatest(*col)).otherwise(col(colDate)))
    else:
        x = x.withColumn(colDate, greatest(*cols))

    return x            

__combine_address_cols = ['uni_establishment_street', 'uni_establishment_number', 'uni_establishment_city']

def df_create_combined_address(x, colCombi = 'fbb_combined_address', cols = __combine_address_cols, replaceCol = True):
    """Get the maximum or minumum date from all specified columns per row.
    
    Description:
        This fucntion gets the maximum or minimim date that appears in 
        the provided columns and NULL values are ignored.
        
    Args:
        x (Spark Dataframe): Spark dataframe containing dates to be used.
        colCombi (String): Column name for (new) column with min/max date.
        cols (List): List of columns to include in row-based comparison.
        replaceCol (Boolean): Create/replace colDate (true, default)
            otherwise update colDate when value is blank.
        
    Returns:
        Spark Dataframe: Returns the dataframe after the computations.
    """
    print(f'    {datetime.datetime.now()} >> df_create_last_update()')
    for c in cols:
        if c not in x.columns:
            raise Exception(f'!! Column {c} is not found in the source data (x).')
    if (colCombi not in x.columns) & (not replaceCol):
        raise Exception(f'!! Date column {colCombi} is not found in the source data (x) and it cannot be created (replaceDateCol = False).')

    # Write code here to get the max or min date of all provided columns
    # and write this one to the newly created or already existing column.
    if not replaceCol:
        x = x.withColumn(colCombi, when(col(colCombi).isNull(), concat(*col)).otherwise(col(colCombi)))
    else:
        x = x.withColumn(colCombi, concat(*cols))

    return x       


########################################################################
### Match and retrieve or create FBB identifiers
########################################################################

def df_sort_columns(x):
    """Sort columns alphabetically.
    
    Description: This function sorts the columns of the dataframe in
        alphabetical order.
    Args:
        x (Spark Dataframe): Dataframe to be sorted.
    """
    print(f'    {datetime.datetime.now()} >> df_sort_columns()')
    x.select(sorted(x.columns))
    return x
    
# __group_id_col = 'case_id'

# def df_lookup_group_ids(x, y, group_id = __group_id_col, overwrite:bool = False):
#     # Now, from the backbone we create a list of distinct case_id's with 
#     # the associated fbb_grp_id. There should not be any duplicates 
#     # (the case_idf and fbb_grp_id should uniquely match).
#     print(f'    {datetime.datetime.now()} >> df_lookup_group_ids()')
#     y = y.select('case_id', 'fbb_grp_id').distinct()
#     y = y.withColumnRenamed('case_id', 'backbone_case_id')
#     y = y.withColumnRenamed('fbb_grp_id', 'backbone_fbb_grp_id')

#     duplicate_cases = int(y.select('backbone_case_id').groupBy('backbone_case_id').count().where(f.col('count') > 1).select(f.sum('count')).collect()[0][0] or 0)    
#     print(f'    {datetime.datetime.now()} :: Duplicate count is: {duplicate_cases}')
    
#     if duplicate_cases > 0:
#         raise Exception(f'There are {duplicate_cases} duplicate case_ids found in the backbone data (y), this should not be allowed, please check te backbone.')
#     if group_id not in x.columns:
#         raise Exception(f'The grouping column {group_id} is not found in the source dataset, no group matching can be executed.')
    
#     x = x.join(y, x[group_id] == y['backbone_case_id'], 'left')
#     if overwrite:
#         x = x.withColumn('fbb_grp_id', col('backbone_fbb_grp_id'))
#     else:
#         x = x.withColumn('fbb_grp_id', coalesce('fbb_grp_id', 'backbone_fbb_grp_id'))
#     x = x.drop('backbone_case_id')
#     x = x.drop('backbone_fbb_grp_id')
#     return x

__test_cols = {
    'case_id' : 'case_id',
    'establishment_id' : 'establishment_id'
    }   

def df_filter_empty_columns(x, filter_cols = __test_cols):
    """Filter out rows that contain an empty string ('').

    Description:
        This fucntion checks in the provided columns if there are rows
        with an empty string or NULL values. These specific rows are
        removed from the dataframe.
    Args: 
        x (Spark Dataframe): Dataframe with the data to be filtered.
    filter_cols (List): List of column names to check for empty strings.
    """
    print(f'    {datetime.datetime.now()} >> df_filter_empty_columns()')
    for c in filter_cols:
        x = x.filter((f.col(c) != '') & (f.col(c).isNotNull()))
    return x

def df_exact_match(x, y, mapping:dict = __test_cols, threshold:int = 0, allow_nulls:bool = False, override_matches:bool = False):
    """Perform exact matching algorithm.
    
    Description: This function finds the appropriate fbb_id, fbb_grp_id,
        fbb_est_id values from the backbone data for the source data. This
        function is generic and can be applied for any type of exact matching.
        
    Args:
        x (Spark Dataframe): The source data to be matched
        y (Spark Dataframe): The backbone that is used for matching
        mapping (dict): Dictionary specification of the source column (key)
            with the corresponding backbone equivalent column (value).
        threshold (int): When only a part of the variables must match, 
            e.g. 2 out of 3 for the match to be made, set this value to 2.
    """
    print(f'    {datetime.datetime.now()} >> df_exact_match()')

    # Ensure that we have all matching columns
    x_cols = mapping.keys()
    y_cols = list(map(lambda x: x, mapping.values()))

    # Check if y has fbb columns
    if x is None:
        raise Exception(f'No source data (x) is provided.')
    if y is None:
        raise Exception(f'No backbone data (y) is provided.')
    for c in __fbb_cols:
        if c not in y.columns: 
            raise Exception(f'Incorrect format of backbone source (y), column {c} is missing.')
    for c in x_cols:
        if c not in x.columns:
            raise Exception(f'Incorrect column in matching source (x), column {c} is not found.')
    for c in y_cols:
        if c not in y.columns:
            raise Exception(f'Incorrect column in backbone source (y), column {c} is not found.')

    ### Prepare the backbone data:
    
    # First we subset to the bare minimum we need:
    # Distinct is not necessary since it's implied by the __fbb_cols already.
    print(f'    {datetime.datetime.now()}    Preparing backbone data...')
    y = y.select(*y_cols, *__fbb_cols)
    if not allow_nulls:
        y = df_filter_empty_columns(y, y_cols)
    # To avoid duplicate column names we rename the y coloms to match the x columns.
    for c in mapping.keys():
        y.withColumnRenamed(mapping.get(c), c)
        
    # To avoid having multiple columns with the same name, we relabel 
    # the backbone merge columns (in y) with y_, but when the fbb columns
    # are also part of the matching columns (x_cols) we won't relabel them...
    new_fbb_cols = []
    for c in __fbb_cols:
        if c in y.columns:
            if c not in x_cols:
                y = y.withColumnRenamed(c, f'y_{c}')
                new_fbb_cols + [f'y_{c}']
            else:
                new_fbb_cols + [c]
                    
    ### Prepare the source data:
    # We cannot enforce that the source table has row_ids so we make them 
    # to ensure uqniueness. This column can be used to identify duplicate
    # matches, which should not happen in general.
    x = x.withColumn('merge_row_id', f.expr('uuid()'))
    x.cache()
    
    # Now let's find exact matches in the current data. 
    # Clearly, since the backbone has historic data, we can get duplicate
    # results. this leads to duplicate merge_row_ids which need to be resolved.
    df = x.join(y, [*x_cols], 'left')
        
    # Create a window function in which we iterate the rows for each
    # unique merge_row_id and y_fbb_id, order by date_deleted. 
    # Then we filter only the rows that are the first row and we drop
    # the row iterator again. If there are multiple different fbb_ids
    # available, we will assume that the latest is the greatest...
    ws  = Window.partitionBy('merge_row_id').orderBy('fbb_deleted', 'fbb_max_date', 'fbb_wave_id')
    df = df.withColumn('row_iterator', row_number().over(ws))
    df = df.filter(col('row_iterator') == 1)
    df = df.drop('row_iterator')
    
    # Sometimes we drop the fbb_id_cols we are performig a new match (so 
    # we don't continue with a partially matched dataframe which we could 
    # have in case of mulitple passes.)

    # Now we can replace all the values if they did not contain a value yet        
    df = df.na.fill('', ['y_fbb_id', 'y_fbb_grp_id', 'y_fbb_est_id'])
    if override_matches:
        df = df.withColumn('fbb_id', when(col('y_fbb_id') != '', col('y_fbb_id')).otherwise(col('fbb_id')))
        df = df.withColumn('fbb_grp_id', when(col('y_fbb_grp_id') != '', col('y_fbb_grp_id')).otherwise(col('fbb_grp_id')))
        df = df.withColumn('fbb_est_id', when(col('y_fbb_est_id') != '', col('y_fbb_est_id')).otherwise(col('fbb_est_id')))
    else:
        df = df.withColumn('fbb_id', when((col('y_fbb_id') != '') & (col('fbb_id') == ''), col('y_fbb_id')).otherwise(col('fbb_id')))
        df = df.withColumn('fbb_grp_id', when((col('y_fbb_grp_id') != '') & (col('fbb_grp_id') == ''), col('y_fbb_grp_id')).otherwise(col('fbb_grp_id')))
        df = df.withColumn('fbb_est_id', when((col('y_fbb_est_id') != '') & (col('fbb_est_id') == ''), col('y_fbb_est_id')).otherwise(col('fbb_est_id')))

    # Cleanup the temporal columns
    pattern = 'y_'
    df = df.select([col(c) for c in df.columns if not re.match(pattern, c)])
    pattern = 'merge_'
    df = df.select([col(c) for c in df.columns if not re.match(pattern, c)])
    # Reorder the columns consistently
    df = df_sort_columns(df)
    df = df_move_to_front(df, __fbb_cols)
    return df

__add_cols = {
    'case_id': 'case_id',
    'uni_establishment_street': 'uni_establishment_street',
    'uni_establishment_number': 'uni_establishment_number',
    'uni_establishment_city': 'uni_establishment_city'
}

def df_generate_ids(x, y = None, id_col = None, key_cols = None, force = False, allow_nulls = False):
    """Generates a list of UUID's unique for the provided column.
    
    Description:
        This function generates for each unique combination of provided
        columns a new UUID. Then, when the column that should contain the
        newly UUID is empty, it will assign the UUID, otherwise the value
        does not change. The function will not make any columns, so they
        must already exist in the dataframe (to ensure consistency).
        
    Args:
        x (Spark Dataframe): Dataframe with the standard fbb columns.
        y (Spark Dataframe): Dataframe with the backbone for addition.
        id_col (String): Contains the name of the field that should 
            contain the newly generated UUID or keep its existing value.
        key_cols (List): Contains a list of the column names that together
            identify the uniqueliness of the UUID column.
        force (Boolean): If this value is set to true (defautl false), the 
            newly generated UUID will be forces to become the column value.
        allow_nulls (Boolean): If this is true (defautl false), NULL values
            and '' values are included in the UUID list.
    """
    print(f'    {datetime.datetime.now()} >> df_generate_fbb_ids()')
    # Checking parameters
    if x is None:
        print(f'    {datetime.datetime.now()} !! No data frame x was specified.')
        raise Exception('!! No data frame x was specified.')        
    if id_col is None:
        print(f'    {datetime.datetime.now()} !! No id_col was specified.')
        raise Exception('!! No id_col was specified.')
    if key_cols is None:
        print(f'    {datetime.datetime.now()} !! No key_cols were specified.')
        raise Exception('!! No key_cols were specified.')
    for c in [id_col, *key_cols]:
        if c not in x.columns:
            print(f'    {datetime.datetime.now()} !! Incorrect specification of id and key columns in source (x): column {c} is missing.')
            raise Exception(f'!! Incorrect specification of id and key columns in source (x): column {c} is missing.')
    if y is not None:
        for c in [id_col, *key_cols]:
            if c not in y.columns:
                print(f'    {datetime.datetime.now()} !! Incorrect specification of id and key columns in backbone (y): column {c} is missing.')
                raise Exception(f'!! Incorrect specification of id and key columns in backbone(y): column {c} is missing.')

    # Generate a UUID for each distinct combination of key_cols
    x_lst = x.select(*key_cols).distinct()
    if not allow_nulls:
        for c in key_cols:
            x_lst = x_lst.filter((f.col(c) != '') & f.col(c).isNotNull())
    x_lst = x_lst.withColumn('x_uuid', f.expr('uuid()'))
    x_lst.cache()
    x = x.join(x_lst, key_cols, how = 'left')
    
    # Retrieve known UUIDs for each distinct combination of key_cols
    if y is not None:
        y_lst = y.select(*key_cols, id_col).distinct()
        if not allow_nulls:
            for c in [*key_cols , id_col]:
                y_lst = y_lst.filter((f.col(c) != '') & f.col(c).isNotNull())
        y_lst = y_lst.withColumnRenamed(id_col, 'y_uuid')
        x = x.join(y_lst, key_cols, how = 'left')
    
    if force:
        if y is None:
            x = x.withColumn(f.col(id_col), when((f.col('x_uuid').isNotNull()) & (f.col('x_uuid') != ''), f.col('x_uuid')).otherwise(f.col(id_col)))
            x = x.drop('x_uuid')
        else:
            x = x.withColumn(f.col(id_col), when(
                (f.col('y_uuid').isNotNull()) & (f.col('y_uuid') != ''), f.col('y_uuid')).otherwise(
                    when((f.col('x_uuid').isNotNull()) & (f.col('x_uuid') != ''), f.col('x_uuid')).otherwise(
                        f.col(id_col))))
            x = x.drop('x_uuid', 'y_uuid')
            y_lst.unpersist()
    else:
        if y is None:
            x = x.withColumn(id_col, when((f.col(id_col) == '') & (f.col('x_uuid').isNotNull()), f.col('x_uuid')).otherwise(f.col(id_col)))
            x = x.drop('x_uuid')
        else:
            x = x.withColumn(id_col, when(
                (f.col(id_col) == '') & (f.col('y_uuid').isNotNull()) & (f.col('y_uuid') != ''), f.col('y_uuid')).otherwise(
                    when((f.col(id_col) == '') & (f.col('x_uuid').isNotNull()) & (f.col('x_uuid') != ''), f.col('x_uuid')).otherwise(
                        f.col(id_col))))
            #x = x.withColumn(id_col, when((f.col(id_col) == '') & (f.col('x_uuid').isNotNull()) & (f.col('x_uuid') != ''), f.col('x_uuid')).otherwise(f.col(id_col)))
            x = x.drop('x_uuid', 'y_uuid')
            y_lst.unpersist()
    x_lst.unpersist()
    return x

def df_generate_fbb_ids(x, print_output = False):
    """Generate new values for fbb_id, fbb_grp_id and fbb_est_id.
    
    Description:
        This function generated UUIDs for all columns required. Both the
        case_id and establishment_id get their unique identifiers but
        also the combination of the two, since it can occur that for
        example a company takes over an existing establishment and in
        that case the est_id remains the same, the grp_id changes to the
        known group, and a new fbb_id is generated.

    Args:
        x (Spark Dataframe): Dataframe with the standard fbb columns.
    """
    print(f'    {datetime.datetime.now()} >> df_generate_fbb_ids()')
    
    # Creating new ID's for all case numbers
    # Technically there should be no listings with no fbb_grp_id (no KVK number)
    print(f'    {datetime.datetime.now()} :: Generating fbb_grp_id values...')
    x = df_generate_ids(x = x, id_col = 'fbb_grp_id', key_cols = ['case_id'], 
                        allow_nulls = False, force = False)
        
    # Creating new ID's for all establishment numbers
    print(f'    {datetime.datetime.now()} :: Generating fbb_est_id values...')
    x = df_generate_ids(x = x, id_col = 'fbb_est_id', key_cols = ['establishment_id'], 
                        allow_nulls = False, force = False)
    
    # Creating new ID's for all combinations of cases with establishments numbers
    print(f'    {datetime.datetime.now()} :: Generating fbb_id values for establishments...')
    x = df_generate_ids(x = x, id_col = 'fbb_id', key_cols = ['case_id', 'establishment_id'], 
                        allow_nulls = False, force = False)
        
    # Yet there are companies with many establishments that do not have 
    # an establishment_id. Giventhat KVK uses the address and SBI code to 
    # determine whether a establishment should get a new establishment_id 
    # we will follow similar logic. But given that there could be many 
    # records that have multiple locations under the same SBI code, this 
    # would overwrite the records each time. Hence we ensure that all records
    # that have the same case_id, fbb_combined_address and code_sbi_1 are
    # considered the same establishment.
    
    print(f'    {datetime.datetime.now()} :: Generating fbb_id values for non-establishments...')
    x = df_generate_ids(x = x, id_col = 'fbb_id', key_cols = ['case_id', 'fbb_combined_address', 'code_sbi_1'], 
                        allow_nulls = False, force = False)
    x = df_generate_ids(x = x, id_col = 'fbb_est_id', key_cols = ['case_id', 'fbb_combined_address', 'code_sbi_1'], 
                        allow_nulls = False, force = False)

    # Finally, if there are any duplicates fbb_id's, we need to invalidate 
    # all but one, we assume that the data has been changed and hence a 
    # newest data must be present somewhere.
    print(f'    {datetime.datetime.now()} :: Enforcing keeping a single active record...')
    w = Window.partitionBy('fbb_grp_id', 'fbb_est_id').orderBy('fbb_max_date')
    x = x.withColumn('fbb_row_number', row_number().over(w))
    x = x.withColumn('fbb_deleted', when((col('fbb_row_number') != 1) & 
                                         (col('fbb_deleted').isNull()), 
                                         current_date()).otherwise(col('fbb_deleted')))
    # Reorder the columns consistently
    x = df_sort_columns(x)
    x = df_move_to_front(x, __fbb_cols)
    return x

def df_append_fbb_ids(x, y):
    """Add values for fbb_id, fbb_grp_id and fbb_est_id.
    
    Description:
        This function generates UUIDs for all columns required when they
        do not have values assigned yet. It creates new fbb_grp_ids for
        all distinct case_ids found in the source data. It also retrieves
        all fbb_grp_ids from the backbone for the given case_ids. When 
        selecting the appropriate fbb_grp_id value, the backbone version
        is preferred over the newly generated identifier. The fbb_id,
        fbb_grp_id and fbb_est_id should be still blank in the x data.

    Args:
        x (Spark Dataframe): Dataframe with the standard fbb columns.
        y (Spark Dataframe): Dataframe with the original backbone.
    """
    print(f'    {datetime.datetime.now()} >> df_append_fbb_id()')
    # Creating new ID's for all case numbers
    # Technically there should be no listings with no fbb_grp_id (no KVK number)
    
    print(f'    {datetime.datetime.now()} :: Generating new fbb_grp_id values...')
    x = df_generate_ids(x = x, y = y, id_col = 'fbb_grp_id', key_cols = ['case_id'], 
                        allow_nulls = False, force = False)

    
    # Creating new ID's for all establishment numbers
    print(f'    {datetime.datetime.now()} :: Updating new fbb_est_id values...')
    x = df_generate_ids(x = x, y = y, id_col = 'fbb_est_id', key_cols = ['establishment_id'], 
                        allow_nulls = False, force = False)

    # Creating new ID's for all combinations of cases with establishments numbers
    print(f'    {datetime.datetime.now()} :: Generating fbb_id values for establishments...')
    x = df_generate_ids(x = x, y = y, id_col = 'fbb_id', key_cols = ['case_id', 'establishment_id'], 
                        allow_nulls = False, force = False)

    # For companies that do not have an establishment_id (due to no core 
    # corporate activities or planned for less than 6 months), we add the 
    # establishment_street, _number and _city to identify uniqueness
    # in the case there is no establishment_id, but there is an fbb_id.
    # We will add fbb_est_id values to ensure that we don't have to do
    # multicolumn matching frequently.
    
    print(f'    {datetime.datetime.now()} :: Generating fbb_id values for non-establishments...')
    x = df_generate_ids(x = x, y = y, id_col = 'fbb_id', key_cols = ['case_id', 'fbb_combined_address', 'code_sbi_1'], 
                        allow_nulls = True, force = False)
    x = df_generate_ids(x = x, y = y, id_col = 'fbb_est_id', key_cols = ['case_id', 'fbb_combined_address', 'code_sbi_1'], 
                        allow_nulls = True, force = False)
    
    # Reorder the columns consistently
    x = df_sort_columns(x)
    x = df_move_to_front(x, __fbb_cols)
    return x

########################################################################
### Deduplicate the dataframe
########################################################################

__full_duplicate_cols = ['case_id', 'establishment_id', 'hq_indicator', 
                         'code_legal_form', 'establishment_dissolved', 'company_dissolved', 'code_status_legal', 
                         'employees_parttime', 'employees_total', 'date_employees', 'employment_total', 'date_employment', 
                         'code_sbi_1', 'code_sbi_2', 'code_sbi_3', 'economically_active', 
                         'registered_corporate_structure', 'indication_import', 'indication_export', 
                         'date_founded', 'date_on_deed', 'date_registration', 'code_registration', 'date_cancellation', 'code_cancellation', 
                         'code_deregistration', 'code_dissolution', 'date_establishment_reported', 'date_initial_establishment', 
                         'date_current_establishment', 'date_continuation', 'date_request_suspension', 'date_active_suspension', 
                         'code_suspension', 'date_request_bankruptcy', 'date_active_bankruptcy', 'code_bankruptcy', 'date_provisioning_report', 
                         'annual_report_year', 'annual_report_type', 'uni_company_name', 'uni_establishment_name', 'uni_partnership_name', 
                         'uni_establishment_street', 'uni_establishment_number', 'uni_establishment_city', 'uni_correspondence_street', 
                         'uni_correspondence_number', 'uni_correspondence_city'
                  ]

def df_drop_duplicates(x, cols = __full_duplicate_cols):
    """Dropping all duplicate rows according to the provided columns.
    
    Description: This function removes all duplicated rows from the
        dataframe, but make sure that this is not done on e.g. the backbone
        data, since this might contain columns that are not included in the
        duplicste check that could have been changed. In the case of the
        backbone you always need to ensure that old data is invalidated in
        case of a change. The number of duplicates found is printed.
        
    Args:
        x (Spark Dataframe): The data to be de-duplicated.
        cols (list): Identify columns which determine the uniqueness
    """
    print(f'    {datetime.datetime.now()} >> df_drop_duplicates()')
    # n_dup = x.groupBy(cols).count().where(f.col('count') > 1).select(f.sum('count')).collect()[0][0]
    # print(f'    {datetime.datetime.now()} :: Drop Duplicates: Number of duplicate rows found: {n_dup}')
    # if n_dup > 0:
    x = x.dropDuplicates(cols)
    return x

# __mark_duplicate_cols = ['case_id', 'establishment_id', 'uni_establishment_street', 'uni_establishment_number', 'uni_establishment_city', 'code_sbi_1']
__mark_duplicate_cols = ['case_id', 'fbb_combined_address', 'code_sbi_1']

def df_renumber_duplicates(x, cols = __mark_duplicate_cols, allow_nulls = False, show_duplicates = False, print_output = False, temp_save_id=0):
    """Renumber existing identifiers in case an alternative row is availalbe.
    
    Description: When multiple duplicates are available in the backbone data
        we need to ensure that the old data is invalidated. Also, the oldest
        possible identifier must be used in order to keep historical records.
        This function will therefore based on the provided criteria (cols)
        take the currently used identifiers to reuse (which recursively did
        the same for older updates so should be the oldest identifier).
        Officially according to the KVK the establishment code only changes
        when both the address have changed and the SBI code at the same time.
        We still add the establishment_id despite that this is often incorrect
        due to not having one (when we use street, nubmer, city and SBI) 
        because the establishment_id should be '' anyway for thoes that need 
        updating and if they do have a different establishent_id given by the
        KVK, who are we to overrule this?
    """
    print(f'    {datetime.datetime.now()} >> df_renumber_duplicates(cols = [{cols}])')
    
    # Check that all referred columns are actually in both data frames.
    for c in cols:
        if c not in x.columns:
            print(f'    {datetime.datetime.now()} !! Incorrect specification of duplicates: column {c} is missing.')
            raise Exception('!! Incorrect specification of duplicates, column is missing.')
    for c in __fbb_cols:
        if c not in x.columns:
            print(f'    {datetime.datetime.now()} !! Incorrect format of source file, backbone column {c} is missing.')
            raise Exception('!! Incorrect format of source file, backbone column is missing.')
    
    # Given that the backbone will be updated over time, we need to keep 
    # the oldest fbb_id alive (and not remove the old record, just mark)
    # So based on the provided columns (cols) we count the duplicates and
    # get the earliest registered fbb_updated date. This row contains 
    # the fbb_est_id and fbb_grp_id that we need to use.
    
    # Add: .filter(col('fbb_deleted').isNull()) because we must have at
    # leat 2 columns with no value in the NULL. Also this value should
    # have come from the previous row-versions so the fbb_id should be
    # correct.
    
    # Working on matching the distinct fbb_id's does not work: when the
    # file contains the same case_id and establishment_id multiple times
    # the assigned fbb_id is the same for multiple rows (correct behavior)
    # yet, for both rows the replacement_fbb_id is equal to the fbb_id,
    # hence when updateing the fbb_deleted date would go wrong, so use
    # the fbb_row_id instead. If there is not difference in fbb_updated,
    # we just have to choose one (even though the )
    #.filter(col('fbb_deleted').isNull()) \
    
    x_dup = x \
        .groupBy(cols) \
            .agg(
                f.count('*').alias('duplicate_count'),
                f.expr("max_by(fbb_row_id, fbb_wave_id)").alias('latest_fbb_row_id'),
                f.expr("min_by(fbb_id, fbb_wave_id)").alias('replacement_fbb_id'),
                f.expr("min_by(fbb_est_id, fbb_wave_id)").alias('replacement_fbb_est_id'),
                f.expr("min_by(fbb_grp_id, fbb_wave_id)").alias('replacement_fbb_grp_id')) \
                    .filter(col('duplicate_count') > 1)

    # Sometimes we have NULL as value in a column which should be '' (otherwise the fiter won't work)
    for c in cols:
        x_dup = x_dup.withColumn(c, when(col(c).isNull(), f.lit('')).otherwise(col(c)))
    for c in ['latest_fbb_row_id', 'replacement_fbb_id', 'replacement_fbb_est_id', 'replacement_fbb_grp_id']:
        x_dup = x_dup.withColumn(c, when(col(c).isNull(), f.lit('')).otherwise(col(c)))
        
    if temp_save_id != 0:
        print(f'    {datetime.datetime.now()} :: Temporarily storing duplicates found in parquet file: x_dup_export_{temp_save_id}...')
        x_dup.repartition(1).write.mode('overwrite').parquet(f'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/temp/x_dup_export_{temp_save_id}')

    # We sometimes enforce that the columns in the cols list must not be null
    # This is essential since, for example, when an establishment moves
    # to another address, you will get twice the establishment_id with
    # different addresses. These must be properly invalidated since this
    # is certainly an duplicte. Yet, if there is no establishment_id
    # then having multiple rows on different addresses would probably
    # indicate multuiple locations (despite not having establishment_id)
    # and then we need to have unique identifiers. So therefore we have
    # a two-pass process, first on case_id and establishment_id with the
    # option allow_nulls = False and then another pass whith all columns
    # where nulls are allowed (but now the copy has a fbb_deleted so it
    # will not recognize as duplicate anymore).
    if print_output: print(f'    {datetime.datetime.now()} :: Of all {x.count()} rows, a total of {x_dup.count()} duplicate cobinations were retrieved.')

    # Remove rows when NULL values are not allowed in identity columns
    if (not allow_nulls):
        for c in cols:
            print(f'    {datetime.datetime.now()} :: Removing rows with NULL values in column: {c}')
            x_dup = x_dup.filter((col(c).isNotNull()) & (col(c) != ''))
            if print_output: print(f'    {datetime.datetime.now()} :: Leaving dataframe with {x_dup.count()} duplicated observations.')

    # Next we add the duplicates count to the backbone data for easy 
    # updating of the fbb_est_id and fbb_grp_id later on.
    x = x.join(x_dup, [*cols], 'left')
    x = x.withColumn('is_latest_row', col('fbb_row_id') == col('latest_fbb_row_id'))
    # This results in 3 possible values for is_latest_row: True, False and NULL
    x.cache()
    
    # Having the data prepared, we need to replace the fbb_id with the 
    # replacement_fbb_id for each row that has a replacement available
    # and we need to update the fbb_deleted column with the current date
    # (as to indicate that this backbone is not available anymore)
    
    # Update all the dates that have a matching replacement record but are not the latest
    x = x.withColumn('fbb_deleted', 
                     when(
                         col('is_latest_row') == False, 
                         current_date()).otherwise(col('fbb_deleted')))

    # After having set a deleted date, we can replace the fbb_id with 
    # the replacent_fbb_id.
    x = x.withColumn('fbb_id', 
                     when((col('replacement_fbb_id') == '') | 
                          (col('replacement_fbb_id').isNull()), 
                          col('fbb_id')).otherwise(col('replacement_fbb_id')))
    x = x.withColumn('fbb_est_id', 
                     when((col('replacement_fbb_est_id') == '') | 
                          (col('replacement_fbb_est_id').isNull()), 
                          col('fbb_est_id')).otherwise(col('replacement_fbb_est_id')))
    x = x.withColumn('fbb_grp_id', 
                     when((col('replacement_fbb_grp_id') == '') | 
                          (col('replacement_fbb_grp_id').isNull()), 
                          col('fbb_grp_id')).otherwise(col('replacement_fbb_grp_id')))
    
    # if temp_save_id != 0:
    #     print(f'    {datetime.datetime.now()} :: Temporarily storing joined data found in parquet file: x_dup_join_export_{temp_save_id}...')
    #     x_dup.repartition(1).write.mode('overwrite').parquet(f'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/temp/x_dup_join_export_{temp_save_id}')

    # Cleanup the temporal columns - We re-run the script multiple times
    x = x.drop('duplicate_count', 'latest_fbb_row_id', 'replacement_fbb_id', 'replacement_fbb_est_id', 'replacement_fbb_grp_id', 'is_latest_row')
    # Now let's check if there are no more duplicates
    if show_duplicates:
        print(f'    {datetime.datetime.now()} :: Showing duplciate fbb_id values (should be nothing):')
        x.filter(x.fbb_deleted.isNull()).groupBy(['fbb_id']).count().where(f.col('count') > 1).show(50)
    return x

def df_renumber_duplicates_by_id(x, file_id = None):
    print(f'    {datetime.datetime.now()} >> df_renumber_duplicates_by_id()')
    window_spec = Window.partitionBy('case_id', 'establishment_id').orderBy(col('fbb_wave_id').asc()) # .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    if file_id is None:
        print(f'    {datetime.datetime.now()}    No file history, taking the first identifier as root.')
        # We do not have a history of file_id's (or we just don't care) take the first row regardless of the data, since all rows are updated they end-up the same anyway.
        x = x.withColumn('fbb_id_update', first('fbb_id', ignorenulls=True).over(window_spec)) \
            .withColumn('fbb_est_id_update', first('fbb_est_id', ignorenulls=True).over(window_spec)) \
                .withColumn('fbb_id', when((col('establishment_id') != ""), col('fbb_id_update')).otherwise(col('fbb_id'))) \
                    .withColumn('fbb_est_id', when((col('establishment_id') != ""), col('fbb_est_id_update')).otherwise(col('fbb_est_id'))) \
                        .drop("fbb_id_update", "fbb_est_id_update")

    else:    
        print(f'    {datetime.datetime.now()}    Current file is provided, taking an older identifier as root.')
        # This is typical for updating, so we have old data and we don't want to renumer these, so we use them specifically.
        x = x.withColumn('fbb_id_update', first(when((col('fbb_file_id') != file_id), col('fbb_id')), ignorenulls=True).over(window_spec)) \
            .withColumn('fbb_est_id_update', first('fbb_est_id', ignorenulls=True).over(window_spec)) \
                .withColumn('fbb_id', when(
                    (col('fbb_file_id') == file_id) & (col('establishment_id') != ""), 
                    col('fbb_id_update')).otherwise(col('fbb_id'))) \
                        .withColumn('fbb_est_id', when(
                            (col('fbb_file_id') == file_id) & (col('establishment_id') != ""), 
                            col('fbb_est_id_update')).otherwise(col('fbb_est_id'))) \
                                .drop("fbb_id_update", "fbb_est_id_update")
        x = x.withColumn('fbb_deleted', when(
            (col('fbb_deleted').isNull()) & 
            (col('fbb_file_id') != file_id) & 
            (col('establishment_id') != ""), 
            current_date()).otherwise(col('fbb_deleted')))
    return x

def df_renumber_duplicates_by_other(x, file_id = None):
    print(f'    {datetime.datetime.now()} >> df_renumber_duplicates_by_other()')
    window_spec = Window.partitionBy('case_id', 'establishment_id').orderBy(col('fbb_wave_id').asc()) # .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    if file_id is None:    
        print(f'    {datetime.datetime.now()}    No file history, taking the first identifier as root.')
        
        matching_condition = (
            (when(concat(f.col('uni_establishment_street'), f.col('uni_establishment_number'), f.col('uni_establishment_city')) == f.expr(f'first(concat(uni_establishment_street, uni_establishment_number, uni_establishment_city), ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            # (when(col('uni_establishment_number') == f.expr(f'first(uni_establishment_number, ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            # (when(col('uni_establishment_city') == f.expr(f'first(uni_establishment_city, ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            (when(f.col('code_sbi_1') == f.expr(f'first(code_sbi_1, ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            (when(f.col('uni_company_name') == f.expr(f'first(uni_company_name, ignorenulls=true)').over(window_spec), 1).otherwise(0))
        )
    
        # We do not have a history of file_id's (or we just don't care) take the first row regardless of the data, since all rows are updated they end-up the same anyway.
        x = x.withColumn('fbb_id_update', first(when(matching_condition >= 2, f.col('fbb_id')), ignorenulls=True).over(window_spec)) \
            .withColumn('fbb_est_id_update', first(when(matching_condition >= 2, f.col('fbb_est_id')), ignorenulls=True).over(window_spec)) \
                .withColumn('fbb_id', when((f.col('establishment_id') != ""), 
                                           f.col('fbb_id_update')).otherwise(f.col('fbb_id'))) \
                                               .withColumn('fbb_est_id', when((f.col('establishment_id') != ""), 
                                                                              f.col('fbb_est_id_update')).otherwise(f.col('fbb_est_id'))) \
                                                                                  .drop("fbb_id_update", "fbb_est_id_update")
    else:    
        print(f'    {datetime.datetime.now()}    Current file is provided, taking an older identifier as root.')
        # This is typical for updating, so we have old data and we don't want to renumer these, so we use them specifically.
        matching_condition = (
            (when(concat(f.col('uni_establishment_street'), f.col('uni_establishment_number'), f.col('uni_establishment_city')) == f.expr(f'first(when(col(fbb_file_id) != {file_id}, concat(uni_establishment_street, uni_establishment_number, uni_establishment_city)), ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            (when(f.col('code_sbi_1') == f.expr(f'first(when(col(fbb_file_id) != {file_id}, code_sbi_1), ignorenulls=true)').over(window_spec), 1).otherwise(0)) +
            (when(f.col('uni_company_name') == f.expr(f'first(when(col(fbb_file_id) != {file_id}, uni_company_name), ignorenulls=true)').over(window_spec), 1).otherwise(0))
        )

        x = x.withColumn('fbb_id_update', first(when((f.col('fbb_file_id') != file_id) & (matching_condition >= 2), f.col('fbb_id')), ignorenulls=True).over(window_spec)) \
            .withColumn('fbb_est_id_update', first(when((f.col('fbb_file_id') != file_id) & (matching_condition >= 2), f.col('fbb_est_id')), ignorenulls=True).over(window_spec)) \
                .withColumn('fbb_id', when(
                    (f.col('fbb_file_id') == file_id) & (f.col('establishment_id') != ""), 
                    f.col('fbb_id_update')).otherwise(f.col('fbb_id'))) \
                        .withColumn('fbb_est_id', when(
                            (f.col('fbb_file_id') == file_id) & (f.col('establishment_id') != ""), 
                            f.col('fbb_est_id_update')).otherwise(f.col('fbb_est_id'))) \
                                .drop("fbb_id_update", "fbb_est_id_update")
        x = x.withColumn('fbb_deleted', when(
            (f.col('fbb_deleted').isNull()) & 
            (f.col('fbb_file_id') != file_id) & 
            (f.col('establishment_id') != ""), 
            current_date()).otherwise(f.col('fbb_deleted')))
    return x


########################################################################
### Append or Update Backbone
########################################################################

__bb_cols = ['fbb_id', 'fbb_grp_id', 'fbb_est_id', 'fbb_row_id', 'fbb_file_id', 'fbb_wave_id', 'fbb_updated', 'fbb_deleted', 'fbb_max_date', 'fbb_source', 'fbb_combined_address', 
             'case_id', 'establishment_id', 'hq_indicator', 'company_name', 'establishment_name', 'partnership_name', 'code_legal_form', 'description_legal_form', 
             'establishment_dissolved', 'company_dissolved', 'code_status_legal', 
             'establishment_street', 'establishment_number', 'establishment_extension', 'establishment_postcode', 'establishment_pc4', 'establishment_city', 'establishment_code_municipality', 
             'correspondence_street', 'correspondence_number', 'correspondence_extension', 'correspondence_postcode', 'correspondence_pc4', 'correspondence_city', 'correspondence_code_municipality', 'correspondence_code_country', 
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

def df_update_backbone(x, y):
    """Update the backbone with new data
    
    Description: This procedure will add new records to the backbone
    assuming that the required fbb_columns are already filled out and
    a boolean column to identify new data is present.
    
    Args:
        x (Spark Dataframe): The data that needs to be added.
        y (Spark Dataframe): The backbone data will be appended.
    """
    print(f'    {datetime.datetime.now()} >> df_update_backbone()')
    # First we check if all required columns are available
    print(f'    {datetime.datetime.now()} :: Checking for mandatory backbone columns.')
    for c in __bb_cols:
        if c not in x.columns:
            print(f'    {datetime.datetime.now()} !! The mandatory column {c} is missing in the source data. Cancel appending backbone operation.')
            exit()
        if c not in y.columns:
            print(f'    {datetime.datetime.now()} !! The mandatory column {c} is missing in the backbone data. Please ensure the proper backbone is loaded.')
            exit()
            
    print(f'    {datetime.datetime.now()} :: Filtering backbone columns.')
    x = x.select(__bb_cols)
    # This should not be needed, but just to make sure...
    y = y.select(__bb_cols)
    x = x.union(y)
    # This union will contain many duplicates!!!
    return x

def df_clear_bb_duplicates(x):
    """Clear leftover duplicates from backbone
    
    Description:
        In some cases ther are errors in the KVK data which cannot be
        resolved automatically. Specifically for churches and their
        associated foundations, there are sometimes errors and not dates
        of any kind are registered. Hence we choose to omit these from
        the backbone. Possibly the next data delivery will include them
        correctly. This function returns the corrected dataframe first
        and the erroneous dataframe last (x, y). 
    
    Args:
        x (Spark DataFrame): The backbone dataframe.
        
    Returns:
        x (Spark Dataframe): The corrected datafrane.
        y (Spark Dataframe): The reported errors dataframe.
    """
    print(f'    {datetime.datetime.now()} >> df_clear_bb_duplicates()')    
    
    # Define the window specification for sorting
    window_spec = Window.partitionBy('fbb_grp_id', 'fbb_est_id').orderBy(
        col('fbb_wave_id').desc(), col('fbb_max_date').desc()
    )

    # Add a row number column to identify the first row per key combination
    x = x.withColumn("fbb_row_num", row_number().over(window_spec))

    # Update invalidated_date for rows that are not the first
    x = x.withColumn('fbb_deleted',
        when((col('fbb_row_num') != 1) & col('fbb_deleted').isNull(), current_date())
        .otherwise(col('fbb_deleted'))
    ).drop('fbb_row_num')
    
    # Now we can separate the bad ones from the good ones...
    print(f'    {datetime.datetime.now()} :: Finding duplicate rows.')
    dup_rows = x.filter(col('fbb_deleted').isNull()).groupBy('fbb_grp_id', 'fbb_est_id').count()
    print(f'    {datetime.datetime.now()}    Joining duplicate row counts.')
    x = x.join(dup_rows, ['fbb_grp_id', 'fbb_est_id'], 'left')
    print(f'    {datetime.datetime.now()}    Retrieving duplicate or erroneous rows.')
    y = x.filter((col('count') > 1) | 
                 (col('count').isNull()) |
                 (col('case_id').cast('int').isNull())).drop('count')
    print(f'    {datetime.datetime.now()}    Dropping duplicate rows from backbone.')
    x = x.filter((col('count') < 2) & (col('count').isNotNull())).drop('count')

    return x, y

########################################################################
### Tool to quickly show some data
########################################################################

def rematch_row_ids(x, y, cols = __fbb_cols):
    # We're updating the correct fbb_cols based on matching the fbb_row_ids
    # Technically all row_id's should be present, we should not have filtered the duplicate rows.
    y = y.select(*cols)
    for c in y.columns:
        if c != 'fbb_row_id':
            y = y.withColumnRenamed(c, f'y_{c}')
    x = x.join(y, 'fbb_row_id', 'left')
    for c in cols:
        if c != 'fbb_row_id':
            x = x.withColumn(c, f.col(f'y_{c}'))
    for c in cols:
        x = x.drop(f'y_{c}')
    return x
