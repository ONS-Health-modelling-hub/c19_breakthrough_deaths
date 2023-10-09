## This file acts as the main script for the Breakthrough Deaths iteration of the 
#autumn boosters project. 

##to do's re-run with new data (mortality and v4 of gdppr) - update line 51 to be may date
#-----------------------
# Load libraries
#-----------------------

library(dplyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(ggplot2)
library(survival)
library(splines)
library(broom)
library(sparklyr)
library(magrittr)
library(stringi)
library(rlang)
library(tidyr)



#--------------------------
# Set up the spark connection
#--------------------------

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")


start_date = "2022-09-01"

source("<path>/functions.R")


############################################################
frac_death = 1
frac_alive = 0.05

sample_flow_file_name <- "sample_flow_breakthrough_autumn22"

df_cen_dth <- sdf_sql(sc,"SELECT * FROM <dataset>") 

### filter to linkage flag == 1 (drops people with no nhs number and also ~50k who link to 
# more than one nhs number)

sample_flow <- tibble(stage = "total sample in full join, Census 21 who link to PDS and GDPPR and mort",
                      count = sdf_nrow(df_cen_dth))

#total sample who are in cen21 spine - full join on GDPPR, HES data so drop those record who are not in census
df_cen_dth <- df_cen_dth %>% 
	filter(!is.na(census_id))

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "total sample of people with Cen21 ID",
                          count = sdf_nrow(df_cen_dth)))

df_cen_dth <- df_cen_dth %>%
	mutate(dod = to_date(dod_deaths, "yyyyMMdd")) 

max_dod <- df_cen_dth %>% filter(!is.na(dod)) %>% summarize(max(dod)) %>% collect()

##set end of study date
eos_date <- max_dod - 15

eos_date <- eos_date[1,1]

print(eos_date)

#PHE vaccinations

##vaccinaions filename
filename = "phe_vaccinations_v4_2023_03_02"

#phe vax data
df_vacc <- sdf_sql(sc, paste0("SELECT * FROM <dataset location>", filename)) 

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "total sample in PHE vaccinations data",
                          count = sdf_nrow(df_vacc)))

#"Autumn22" From 01/09/22 (start date of the autumn booster campaign) - 
#the next vaccination event 84 days after the last clinically 
#acceptable dose, provided they have had at least 2 doses.

df_vacc <- df_vacc %>% 
	filter(dv_dose == "Autumn22")

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "total sample in PHE with Autumn22 booster",
                          count = sdf_nrow(df_vacc)))

##join autumn vax onto census 21
df_cen21 <- left_join(df_vacc, df_cen_dth, by = c("nhsnumber" = "nhs_number"))

#use sex as proxy for completing census
df_cen21 <- df_cen21 %>%
  filter(!is.na(sex))

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "total sample of people with Autumn booster in PHE who link to Cen21",
                          count = sdf_nrow(df_cen21)))


df_join <- df_cen21 %>%
	filter(usual_resident_ind == 1)

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "population who are usual residents at time of census",
                          count = sdf_nrow(df_join)))

#selection of population 

#filter to include adults age 50 to 100
df_join <- df_join %>%
	filter(dv_agedateadministered_yrs >= 50 & dv_agedateadministered_yrs <= 100)

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "population aged 50 to 100 years of age on vaccination date",
                          count = sdf_nrow(df_join)))


##filter to include people who have dod after start or before end or are alive

#df_join <- df_join %>%
#	filter(is.na(dod) | dod >= start_date & dod <= eos_date)

df_join <- df_join %>%
	filter(is.na(dod) | dod >= start_date)

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "population alive at start, with no dod or dod between start and eos",
                          count = sdf_nrow(df_join)))

sample_flow %>% print()
#######################################################################################################

# create death involving covid outcome vairable for deaths occurring in study period
# and day on which this occurs
covid_codes <- c("U071", "U072")
df_booster <- df_join %>%
  mutate(death = ifelse(!is.na(dod) & dod <= eos_date, 1, 0)) %>%
    mutate(covid_death = ifelse(death==1 & (!is.na(fic10und_deaths) & fic10und_deaths %in% covid_codes | 
                                            !is.na(fic10men1_deaths) & fic10men1_deaths %in% covid_codes |
         !is.na(fic10men2_deaths) & fic10men2_deaths %in% covid_codes | !is.na(fic10men3_deaths) & fic10men3_deaths %in% covid_codes |
         !is.na(fic10men4_deaths) & fic10men4_deaths %in% covid_codes | !is.na(fic10men5_deaths) & fic10men5_deaths %in% covid_codes |
         !is.na(fic10men6_deaths) & fic10men6_deaths %in% covid_codes | !is.na(fic10men7_deaths) & fic10men7_deaths %in% covid_codes |
         !is.na(fic10men8_deaths) & fic10men8_deaths %in% covid_codes | !is.na(fic10men9_deaths) & fic10men9_deaths %in% covid_codes |
         !is.na(fic10men10_deaths) & fic10men10_deaths %in% covid_codes | !is.na(fic10men11_deaths) & fic10men11_deaths %in% covid_codes |
         !is.na(fic10men12_deaths) & fic10men12_deaths %in% covid_codes | !is.na(fic10men13_deaths) & fic10men13_deaths %in% covid_codes |
         !is.na(fic10men14_deaths) & fic10men14_deaths %in% covid_codes | !is.na(fic10men15_deaths) & fic10men15_deaths %in% covid_codes), 1, 0)) %>%
  mutate(non_covid_death = ifelse(death == 1 & covid_death == 0, 1, 0))

df_booster %>% group_by(covid_death, non_covid_death, death) %>% count() %>% print()

## dose date plus 14 is when time at risk commences, calendar_time diff between dose_date and start_date
df_booster <- df_booster %>% 
	mutate(dose_date = date(timestamp(dv_dateadministered)),
				 dose_date_plus14 = date_add(dose_date, 14),
				 calendar_time_infected = datediff(dose_date, start_date))

df_booster %>% 
	select(dv_dateadministered, dose_date, dose_date_plus14, calendar_time_infected) %>%
	head()

df_booster <- df_booster %>% 
  mutate(time_at_risk = case_when(death == 1 ~ datediff(dod, dose_date_plus14),
                                 death == 0 ~ datediff(eos_date, dose_date_plus14))) %>%
  filter(time_at_risk >= 0)

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sanity check time at risk > 0",
                          count = sdf_nrow(df_booster)))

#there are 9 less covid deaths in this sample meaning there are 9 deaths between vax and 
#14+ days since vax
df_booster %>% group_by(covid_death, non_covid_death, death) %>% count() %>% print()

##variable prep

df_booster <- df_booster %>% 
	select(-dv_postcode_attransfer, -gppracticecode_attransfer, - lsoa11_attransfer,
				-contains("fic"), -ageinyrs_deaths)

#renaming all census variables
df_booster <- df_booster %>% 
  mutate(sex = case_when(sex == 1 ~ "Female",
                         sex == 2 ~ "Male"),
        age_group = case_when(dv_agedateadministered_yrs >= 18 & dv_agedateadministered_yrs <= 29 ~ "18-29",
                              dv_agedateadministered_yrs >= 30 & dv_agedateadministered_yrs <= 39 ~ "30-39",
                              dv_agedateadministered_yrs >= 40 & dv_agedateadministered_yrs <= 49 ~ "40-49",
                              dv_agedateadministered_yrs >= 50 & dv_agedateadministered_yrs <= 59 ~ "50-59",
                              dv_agedateadministered_yrs >= 60 & dv_agedateadministered_yrs <= 69 ~ "60-69",
                              dv_agedateadministered_yrs >= 70 & dv_agedateadministered_yrs <= 79 ~ "70-79",
                              dv_agedateadministered_yrs >= 80 & dv_agedateadministered_yrs <= 90 ~ "80-89",
															dv_agedateadministered_yrs >= 90 ~ "90+"),
         ethnicity = case_when(ethnic19_20 == 1 ~ "White British",
                               ethnic19_20 >= 2 & ethnic19_20 <= 5 ~ "White other",
                               ethnic19_20 >= 6 & ethnic19_20 <= 9 ~ "Mixed",
                               ethnic19_20 == 10 ~ "Indian",
                               ethnic19_20 == 11 ~ "Pakistani",
                               ethnic19_20 == 12 ~ "Bangladeshi",
                               ethnic19_20 == 13 ~ "Chinese",
                               ethnic19_20 == 15 ~ "Black Caribbean",
                               ethnic19_20 == 16 ~ "Black African",
                               (ethnic19_20 == 14 | ethnic19_20 == 17 |
                                ethnic19_20 == 18 | ethnic19_20 == 19) ~ "Other"),
         religion_tb = case_when(religion_tb == 1 ~ "No religion",
                                 religion_tb == 2 ~ "Christian",
                                 religion_tb == 3 ~ "Buddhist",
                                 religion_tb == 4 ~ "Hindu",
                                 religion_tb == 5 ~ "Jewish",
                                 religion_tb == 6 ~ "Muslim",
                                 religion_tb == 7 ~ "Sikh",
                                 religion_tb == 8 ~ "Other religion",
                                 religion_tb == 9 ~ "Not stated"),
          ns_sec = case_when(ns_sec >= 1 & ns_sec < 4 ~ "1 Higher managerial, administrative and professional occupations",
                             ns_sec >= 4 & ns_sec < 7 ~ "2 Lower managerial, administrative and professional occupations",
                             #ns_sec == 3 ~ "Higher professional occupations",
                             #ns_sec == 4 ~ "Lower professional and higher technical occupations",
                             #ns_sec == 5 ~ "Lower managerial and administrative occupations",
                             #ns_sec == 6 ~ "Higher supervisory occupations",
                             ns_sec == 7 ~ "3 Intermediate occupations",
                             ns_sec >= 8 & ns_sec < 10 ~ "4 Small employers and own account workers",
                             #ns_sec == 9 ~ "Own account workers",
                             ns_sec == 10 & ns_sec < 12 ~ "5 Lower supervisory and technical occupations", 
                             #ns_sec == 11 ~ "Lower technical occupations",
                             ns_sec == 12 ~ "6 Semi-routine occupations",
                             ns_sec == 13 ~ "7 Routine occupations",
                             ns_sec >= 14 & ns_sec <= 15 ~ "8 Never worked",
                             #ns_sec == 15 ~ "Long-term unemployed",
                             ns_sec == 16 ~ "Full-Time students"),
         disability = case_when(disability == 1 ~ "Yes - reduced a lot",
                                disability == 2 ~ "Yes - reduced a little",
                                disability == 3 ~ "Yes - not reduced at all",
                                disability == 4 ~ "No"),
         health_in_general = case_when(health_in_general == 1 ~ "Very good health",
                                       health_in_general == 2 ~ "Good health",
                                       health_in_general == 3 ~ "Fair health",
                                       health_in_general == 4 ~ "Bad health",
                                       health_in_general == 5 ~ "Very bad health"),
          frontline_health = case_when(substr(soc, 1,3) == 117 ~ 1, # Health and social services managers and directors
                                      substr(soc, 1,3) == 123 ~ 1, # Managers and proprietors in health and care services
                                      substr(soc, 1,3) == 221 ~ 1, # Medical practitioners
                                      substr(soc, 1,3) == 222 ~ 1, # Therapy professionals
                                      substr(soc, 1,3) == 223 ~ 1, # Nursing professionals
                                      substr(soc, 1,3) == 225 ~ 1, # Other health professionals
                                      substr(soc, 1,3) == 321 ~ 1, # Health associate professionals
                                      substr(soc, 1,4) == 4211 ~ 1, # Medical secretaries
                                      substr(soc, 1,3) == 613 ~ 1, # Caring personal services
                                      substr(soc, 1,4) == 9262 ~ 1, # Hospital porters
                                      TRUE ~ 0),
        soc = case_when(substr(soc, 1,2) == 11 ~ "Corporate managers and directors",
                        substr(soc, 1,2) == 12 ~ "Other managers and proprietors",
                        substr(soc, 1,2) == 21 ~ "Science, research, engineering and technology professionals",
                        substr(soc, 1,2) == 22 ~ "Health professionals",
                        substr(soc, 1,2) == 23 ~ "Teaching and other educational professionals",
                        substr(soc, 1,2) == 24 ~ "Business, media and public service professionals",
                        substr(soc, 1,2) == 31 ~ "Science, engineering and technology associate professionals",
                        substr(soc, 1,2) == 32 ~ "Health and social care associate professionals",
                        substr(soc, 1,2) == 33 ~ "Protective service occupations",
                        substr(soc, 1,2) == 34 ~ "Culture, media and sports occupations",
                        substr(soc, 1,2) == 35 ~ "Business and public service associate professionals",
                        substr(soc, 1,2) == 41 ~ "Administrative occupations",
                        substr(soc, 1,2) == 42 ~ "Secretarial and related occupations",
                        substr(soc, 1,2) == 51 ~ "Skilled agricultural and related trades",
                        substr(soc, 1,2) == 52 ~ "Skilled metal, electrical and electronic trades",
                        substr(soc, 1,2) == 53 ~ "Skilled construction and building trades",
                        substr(soc, 1,2) == 54 ~ "Textiles, printing and other skilled trades",
                        substr(soc, 1,2) == 61 ~ "Caring personal service occupations",
                        substr(soc, 1,2) == 62 ~ "Leisure, travel and related personal service occupations",
                        substr(soc, 1,2) == 63 ~ "Community and civil enforcement occupations",
                        substr(soc, 1,2) == 71 ~ "Sales occupations",
                        substr(soc, 1,2) == 72 ~ "Consumer service occupations",
                        substr(soc, 1,2) == 81 ~ "Process, plant and machine operatives",
                        substr(soc, 1,2) == 82 ~ "Transport and mobile machine drivers and operatives",
                        substr(soc, 1,2) == 91 ~ "Elementary trades and related occupations",
                        substr(soc, 1,2) == 92 ~ "Elementary administration and service occupations"),
         english_language_proficiency = case_when(english_language_proficiency == 1 ~ "Very well",
                                                  english_language_proficiency == 2 ~ "Well",
                                                  english_language_proficiency == 3 ~ "Not well",
                                                  english_language_proficiency == 4 ~ "Not at all",
                                                  english_language_proficiency == -8 ~ "Code not required")) 



##add in Census21 geographies ###############################################################

census21_OA_path <- "2021_census_furd_sdc.furd_2a_v3"
cen21_spine_pds_path <- "cen_dth_gps.cen21_spine_pds"
census2011_res_path <- "cen_dth_gps.pre_processed_census_2011_std"
oa_to_region_lookup_path <- paste0("<path to file>/oa_lsoa_msoa_ew_dec_2021_lu_v5.csv")

oa_to_region_lookup <- spark_read_csv(sc, path = oa_to_region_lookup_path)
oa_to_region_lookup <- oa_to_region_lookup %>% select(oa21cd,rgn22nm)

census21_OA <- sdf_sql(sc, paste0("SELECT * FROM ", census21_OA_path))
census21_OA <- census21_OA %>% select(response_id, output_area)


# Add region code for filtering
final_oa <- census21_OA %>%
  select(response_id, census21_oa = output_area) %>%
  left_join(oa_to_region_lookup, by = c("census21_oa" = "oa21cd"))

final_oa <- final_oa %>%
  # select necessary variables
  select(response_id,
         region = rgn22nm)

df_booster <- df_booster %>% 
  left_join(final_oa, by = "response_id")

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sanity count of joining region",
                          count = sdf_nrow(df_booster)))

sample_flow %>% collect %>% print()

#df_booster %>% group_by(region) %>% count() %>% print()


run_date <- Sys.Date()
run_date <- gsub('-', '_', run_date)

write.csv(sample_flow, paste0("<path>", run_date, ".csv"))


##sampling #####################################################################
#store all deaths
df_linked_death <- df_booster %>% 
  filter(death == 1) %>%
  mutate(weight = 1/frac_death)

df_linked_alive <- df_booster %>%
  filter(death == 0) %>%
  sdf_sample(fraction = frac_alive, replacement=FALSE, seed = 1) %>%
  mutate(weight = 1/frac_alive)

df_sample <- rbind(df_linked_death, df_linked_alive)

## collect data here as rds
df_sample <- df_sample %>%
  collect() 

##read in RDS from memory 
saveRDS(df_sample, paste0("<path>", run_date, ".RDS"))


##if running demog count tables include the below:###########################################

##add in sociodemo tables 
df_booster <- df_booster %>% 
	mutate(cause_death = case_when(covid_death == 1 & death == 1 ~ "Covid19death",
														   		non_covid_death == 1 & death == 1 ~ "NonCovid19death", 
															 		death == 0 ~ "Alive"))

df_booster %>% summarize(mean(dv_agedateadministered_yrs), sd(dv_agedateadministered_yrs))

##mean and SD of deaths
df_covid <- df_booster %>% 
	filter(cause_death == "Covid19death")

df_covid %>% summarize(mean(dv_agedateadministered_yrs), sd(dv_agedateadministered_yrs))

df_noncovid <- df_booster %>% 
	filter(cause_death == "NonCovid19death")

df_noncovid %>% summarize(mean(dv_agedateadministered_yrs), sd(dv_agedateadministered_yrs))


# replace na
df_booster <- df_booster %>% 
   replace_na(list(MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag=0, 
                   leukolaba_gpes_flag=0, LiverCirrhosis_gpes_flag=0,Asthma_gpes_flag=0, 
                   PriorFractureOfHipWristSpineHumerus_gpes_flag=0, Diabetes_gpes_flag=0, SCID_gpes_flag=0,
                   SevereMentalIllness_gpes_flag=0, Epilepsy_gpes_flag=0, CysticFibrosisBronchiectasisAlveolitis_gpes_flag=0, 
                   HeartFailure_gpes_flag=0, LungOrOralCancer_gpes_flag=0, PulmonaryHypertensionOrFibrosis_gpes_flag=0, 
                   IBD_gpes_flag=0, prednisolone_gpes_flag=0,
                   HIV_gpes_flag=0, hba1c_gpes_flag=0, PeripheralVascularDisease_gpes_flag=0,
                   Dementia_gpes_flag=0, DiabetesT2_gpes_flag=0, ChronicKidneyDisease_gpes_flag=0, 
                   DiabetesT1_gpes_flag=0, AtrialFibrillation_gpes_flag=0, schiz_gpes_flag=0,
                   CancerOfBloodOrBoneMarrow_gpes_flag=0, Copd_gpes_flag=0,RheumatoidArthritisOrSle_gpes_flag=0,
                   CerebralPalsy_gpes_flag=0, ParkinsonsDisease_gpes_flag=0, LearningDisabilityOrDownsSyndrome_gpes_flag=0,
                   HousingCategory_gpes_flag=0, ThrombosisOrPulmonaryEmbolus_gpes_flag=0, StrokeOrTia_gpes_flag=0,
                   immunosuppressants_gpes_flag=0, CongenitalHeartProblem_gpes_flag=0, CoronaryHeartDisease_gpes_flag=0)) %>%
    mutate(bmi_group = case_when(is.na(bmi) | bmi <= 5 ~ "Missing",
                               bmi > 6 & bmi < 18.5 ~ "Underweight", 
                               bmi >= 18.5 & bmi < 25 ~ "Ideal", 
                               bmi >= 25 & bmi < 30 ~ "Overweight", 
                               bmi >= 30 & bmi < 40 ~ "Obese", 
                               bmi >= 40 ~ "Morbidly Obese"))

demog_varsnames <- c("sex",
							"ethnicity",
							"age_group",
							"region",
							"disability",
							"bmi_group",
               "MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag",
               "leukolaba_gpes_flag",
							 "LiverCirrhosis_gpes_flag",
								"Asthma_gpes_flag",
								"PriorFractureOfHipWristSpineHumerus_gpes_flag",
								"SevereMentalIllness_gpes_flag",
								"Epilepsy_gpes_flag",
								"CysticFibrosisBronchiectasisAlveolitis_gpes_flag",
								"HeartFailure_gpes_flag",
								"LungOrOralCancer_gpes_flag",  
								"PulmonaryHypertensionOrFibrosis_gpes_flag",
								"PeripheralVascularDisease_gpes_flag",
								"Dementia_gpes_flag",
								"DiabetesT2_gpes_flag",
								"ChronicKidneyDisease_gpes_flag",
								"DiabetesT1_gpes_flag",
								"AtrialFibrillation_gpes_flag",
								"schiz_gpes_flag",
								"CancerOfBloodOrBoneMarrow_gpes_flag",
								"Copd_gpes_flag",
								"RheumatoidArthritisOrSle_gpes_flag", 
								"ParkinsonsDisease_gpes_flag",
								"LearningDisabilityOrDownsSyndrome_gpes_flag",
								"ThrombosisOrPulmonaryEmbolus_gpes_flag",
								"StrokeOrTia_gpes_flag",
								"immunosuppressants_gpes_flag",
								"CongenitalHeartProblem_gpes_flag",
								"CoronaryHeartDisease_gpes_flag")


## function to calculate counts of deaths and casue
crosstabs_rate_fun <- function(data, by_var, total) {
  crosstabs_table <- data %>%
  group_by(.data[[by_var]]) %>%
  count() %>%
  dplyr::rename(group = all_of(by_var)) %>%
  mutate(domain = by_var) %>%
  ungroup() %>%
  select(group, domain, n) 
  
	print(crosstabs_table)

  death_rates <- data %>% 
  group_by(.data[[by_var]], cause_death) %>% 
  count() %>%
  dplyr::rename(group = all_of(by_var)) %>%
  filter(cause_death == "Covid19death" | cause_death == "NonCovid19death") %>%
  dplyr::rename(totalevents = n)
	
	print(death_rates)
  
  jointable <- merge(crosstabs_table, death_rates) %>%
  select(group, n, cause_death, totalevents, domain) %>%
  pivot_wider(names_from = cause_death, values_from = totalevents) %>%
  dplyr::rename(totalsample = n)

	return(jointable)
	
}

demog_table_deaths <- purrr::pmap_dfr(.l = list(data = purrr::map(1:length(demog_varsnames), function(e) {return(df_booster)}),
                                           by_var = demog_varsnames,
                                           total = purrr::map(1:length(demog_varsnames), function(e) {return(sdf_nrow(df_booster))})),
                                          .f = crosstabs_rate_fun) 




write.csv(demog_table_deaths, paste0("<path>", run_date, ".csv"))


