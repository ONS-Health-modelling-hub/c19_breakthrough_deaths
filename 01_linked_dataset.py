### Set up
sys.path.insert(0, r'<path>')
sys.path.insert(0, r'<path>')
import pysparkFunctions as pf
import pyspark.sql.functions as F
import GDPPR_sfpl_processing as gp
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, FloatType
import re

spark = (
    SparkSession.builder.appName("xl-session")
    .config("spark.executor.memory", "60g")
    .config("spark.yarn.executor.memoryOverhead", "3g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 12)
    .config("spark.sql.shuffle.partitions", 240)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)


os.chdir('<directory>')

### User Inputs

## Directories
cen21_spine_path = '<path>'
snomed_data_frame_path = '<path>' 

## Parameters

# gp.get_gdppr_person_level_data_lookup_as_dataframe
GPES_data_loc = '<database>.gpes_gdppr_v4_std'
snomed_column_name = 'ConceptId'
condition_column_name = 'name'
GPES_date_range_low = datetime(2015, 1, 1)
GPES_date_range_high = datetime(2021, 3, 21)

# BMI
GPES_DATA = '<database>.gpes_gdppr_v4_std'
start_date = GPES_date_range_low
end_date = GPES_date_range_high
output_name = '<database>.analytical_ve_2021census_bmi'


# pf.save_file_in_SQL
prefix = "analytical_breakthroughdeaths"
file_name = "cen21_spine_gpes_flags"
suffix = True
project = '<project>'

## Functions
def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []
    while len(stack) > 0:
        parents, df = stack.pop()
        for column_name, column_type in df.dtypes:
            if column_type[:6] == "struct":
                projected_df = df.select(column_name + ".*")
                stack.append((parents + (column_name,), projected_df))
            else:
                columns.append(F.col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))
    return nested_df.select(columns)

### Create extra large spark session
spark = (
    SparkSession.builder.appName("xl-session")
    .config("spark.executor.memory", "60g")
    .config("spark.yarn.executor.memoryOverhead", "3g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 12)
    .config("spark.sql.shuffle.partitions", 240)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)

### Read in Census 21 spine table
sdf_cen21_spine = spark.read.table(cen21_spine_path).select(F.col('census_id'), F.col('nhs_number'), F.col('frequency_census21_id_linked_to_pds'))

### filter to linkage flag == 1 (drops people with no nhs number and also ~50k who link to 
# more than one nhs number)
sdf_cen21_spine = sdf_cen21_spine.where(F.col("frequency_census21_id_linked_to_pds")==1)

sdf_cen21_spine.count()

### Read in data frame with the codes and condition names

## Use Pandas to read in CSV
snomed_dataframe = spark.createDataFrame(pd.read_csv(snomed_data_frame_path))

## Edit Types for Function
snomed_dataframe = snomed_dataframe.withColumn(
  snomed_column_name,
  F.regexp_replace(snomed_column_name, "s_", "")
  )

### Get GDPPR flags for conditions by nhs_number
sdf_gpes_flags = gp.get_gdppr_person_level_data_lookup_as_dataframe(
    spark,
    GPES_data_loc = GPES_data_loc,
    snomed_dataframe = snomed_dataframe,
    snomed_column_name = snomed_column_name,
    condition_column_name = condition_column_name,
    GPES_date_range_low = GPES_date_range_low,
    GPES_date_range_high = GPES_date_range_high
    )

### Flatten/unnest
sdf_gpes_flags = flatten_df(sdf_gpes_flags)

### Keep necessary columns
sdf_gpes_flags = sdf_gpes_flags.select(
  "nhs_number",
  sdf_gpes_flags.colRegex("`^.*_flag*`")
  )

### Rename: get rid of double underscore
sdf_gpes_flags = sdf_gpes_flags.select([F.col(col).alias(re.sub("__","_",col)) for col in sdf_gpes_flags.columns])
sdf_gpes_flags = sdf_gpes_flags.select([F.col(col).alias(re.sub("_flag","_gpes_flag",col)) for col in sdf_gpes_flags.columns])

### Join GPES flags to Census 21 spine
sdf_final = sdf_cen21_spine.join(
  other = sdf_gpes_flags,
  on = "nhs_number",
  how = "left"
  )

#sdf_final = spark.read.table('cen_dth_gps.analytical_ve_2021census_cen21_spine_gpes_flags_20230327')

# get bmi data
bmi = gp.bmi(
  spark,
  GPES_DATA = GPES_DATA,
  start_date = start_date,
  end_date = end_date,
  output_name = output_name
  )

bmi = spark.read.table('cen_dth_gps.analytical_ve_2021census_bmi')

bmi = bmi.select('nhs_number', 'bmi', 'bmi_category')

sdf_final = sdf_final.join(
  other = bmi,
  on = "nhs_number",
  how = "left"
  )



### Join mortality data to Census 21 spine, only need deaths from 2021 as need to be alive to be in census
sdf_deaths = pf.get_mortality_data(spark,
                        death_start_date="20210101",
                        death_end_date="20230509",
                        latest_death_file="deaths_20230509_std",
                        death_type="registration")

for col in sdf_deaths.columns:
  sdf_deaths = sdf_deaths.withColumnRenamed(col, col + '_deaths')
  
sdf_deaths = sdf_deaths.withColumnRenamed('NHSNO_deaths', 'nhs_number')

sdf_deaths = sdf_deaths.select('nhs_number', 'dor_deaths', 'sex_deaths', 'fic10und_deaths',
                               'fic10men1_deaths', 'fic10men2_deaths',
                               'fic10men3_deaths', 'fic10men4_deaths', 'fic10men5_deaths', 'fic10men6_deaths', 'fic10men7_deaths', 'fic10men8_deaths', 
                               'fic10men9_deaths', 'fic10men10_deaths', 'fic10men11_deaths', 'fic10men12_deaths', 'fic10men13_deaths',
                              'fic10men14_deaths', 'fic10men15_deaths', 'dod_deaths', 'ageinyrs_deaths')

  
sdf_final = sdf_final.join(
  other = sdf_deaths,
  on = "nhs_number",
  how = "left"
  )

### join on resident table via census_id to get sociodemographics

census21_res_path = "2021_census_furd_attributes.furd_2b_resident"
  
census21_res = spark.read.table(census21_res_path).select('resident_id', 'sex', 'highest_qualification', 'religion_tb',
         'ns_sec', 'usual_resident_ind', 'health_in_general', 
         'disability', 'soc', 'english_language_proficiency', 'proxy_answer', 'resident_age', 'ethnic05_20',
                                                         'ethnic19_20', 'ethnic_group_tb', 'residence_type',
                                                         'position_in_ce', 'resident_month_of_birth', 'resident_year_of_birth',
                                                         'ce_id', 'response_id', 'census21_oa')

census21_res.count()

census21_res = census21_res.withColumnRenamed('resident_id', 'census_id')

sdf_final = sdf_final.join(
  other = census21_res,
  on = "census_id",
  how = "left"
  )


### filter no non proxy people with an nhs number
sdf_final = sdf_final.where(F.col("proxy_answer")!=-2)

sdf_columns = sdf_final.columns

#file_name = file_name + "_all_data"

### Save as HIVE table
pf.save_file_in_SQL(
  dataframe = sdf_final,
  prefix = prefix,
  file_name = file_name,
  suffix = suffix,
  project = project
  )