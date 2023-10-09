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
library(parallel)
library(foreach)

source("<path>/functions.R")

#set.seed(1234)

##specify run number (1-10 for each session)
run <- 2

#read in from memory and run in pre-prod

df <- readRDS("<path>/autumn22boosters_2023_06_08.RDS")

totaln <- nrow(df)

#df to store outputs
bootstrap_iterations <- data.frame()
covid_modeloutput <- data.frame()
noncovid_modeloutput <- data.frame()

#specifying model params and vars
#specification of models
cox_surv_covid <- 'Surv(time_at_risk, covid_death)'

cox_surv_noncovid <- 'Surv(time_at_risk, non_covid_death)'

cox_control_2 <- 'ns(dv_agedateadministered_yrs, df=3, Boundary.knots=quantile(dv_agedateadministered_yrs, c(.05, .95))) + sex + ns(caltime, df=3, Boundary.knots=quantile(caltime, c(.05, .95))) + ethnicity + region'

cox_control_6 <- paste(cox_control_2, '+ MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag + leukolaba_gpes_flag + LiverCirrhosis_gpes_flag + Asthma_gpes_flag + PriorFractureOfHipWristSpineHumerus_gpes_flag + SevereMentalIllness_gpes_flag +  Epilepsy_gpes_flag + CysticFibrosisBronchiectasisAlveolitis_gpes_flag + HeartFailure_gpes_flag + LungOrOralCancer_gpes_flag + PulmonaryHypertensionOrFibrosis_gpes_flag + PeripheralVascularDisease_gpes_flag + Dementia_gpes_flag + DiabetesT2_gpes_flag + ChronicKidneyDisease_gpes_flag + DiabetesT1_gpes_flag + AtrialFibrillation_gpes_flag + schiz_gpes_flag + CancerOfBloodOrBoneMarrow_gpes_flag + Copd_gpes_flag + RheumatoidArthritisOrSle_gpes_flag + ParkinsonsDisease_gpes_flag + LearningDisabilityOrDownsSyndrome_gpes_flag + ThrombosisOrPulmonaryEmbolus_gpes_flag + StrokeOrTia_gpes_flag + immunosuppressants_gpes_flag + CongenitalHeartProblem_gpes_flag + CoronaryHeartDisease_gpes_flag')

outcomes_list <- list(cox_surv_covid, cox_surv_noncovid)


# derivation of additional variables
start_date = "2022-09-01"

data_sample <- df %>% 
  mutate(caltime = round(as.numeric(difftime(dose_date, start_date), units="days"), 1))

data_sample <- data_sample %>% 
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

#re-level all categories
data_sample$sex <- as.factor(data_sample$sex)
data_sample$sex <- relevel(data_sample$sex, ref = "Male")

data_sample$bmi_group <- as.factor(data_sample$bmi_group)
data_sample$bmi_group <- relevel(data_sample$bmi_group, ref = "Ideal")

data_sample$health_in_general <- as.factor(data_sample$health_in_general)
data_sample$health_in_general <- relevel(data_sample$health_in_general, ref = "Very good health")

data_sample$disability <- as.factor(data_sample$disability)
data_sample$disability <- relevel(data_sample$disability, ref = "No")

data_sample$ethnicity <- as.factor(data_sample$ethnicity)
data_sample$ethnicity <- relevel(data_sample$ethnicity, ref = "White British")

data_sample$dv_agedateadministered_yrs <- as.numeric(data_sample$dv_agedateadministered_yrs)


comorbidities <- c("MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag1",
                   "leukolaba_gpes_flag1",
                                "LiverCirrhosis_gpes_flag1",
                                "Asthma_gpes_flag1",
                                "PriorFractureOfHipWristSpineHumerus_gpes_flag1",
                                "Diabetes_gpes_flag1",
                                "SCID_gpes_flag1", 
                                "SevereMentalIllness_gpes_flag1",
                                "Epilepsy_gpes_flag1",
                                "CysticFibrosisBronchiectasisAlveolitis_gpes_flag1",
                                "HeartFailure_gpes_flag1",
                                "LungOrOralCancer_gpes_flag1",  
                                "PulmonaryHypertensionOrFibrosis_gpes_flag1",
                                "hba1c_gpes_flag1",
                                "PeripheralVascularDisease_gpes_flag1",
                                "Dementia_gpes_flag1",
                                "DiabetesT2_gpes_flag1",
                                "ChronicKidneyDisease_gpes_flag1",
                                "DiabetesT1_gpes_flag1",
                                "AtrialFibrillation_gpes_flag1",
                                "schiz_gpes_flag1",
                                "CancerOfBloodOrBoneMarrow_gpes_flag1",
                                "Copd_gpes_flag1",
                                "RheumatoidArthritisOrSle_gpes_flag1", 
                                "ParkinsonsDisease_gpes_flag1",
                                "LearningDisabilityOrDownsSyndrome_gpes_flag1",
                                "ThrombosisOrPulmonaryEmbolus_gpes_flag1",
                                "StrokeOrTia_gpes_flag1",
                                "immunosuppressants_gpes_flag1",
                                "CongenitalHeartProblem_gpes_flag1",
                                "CoronaryHeartDisease_gpes_flag1")




##### function to sample my data (move to functions script) #############
sample_df <- function(df){

df_death <- df %>% filter(death == 0)
  
ids_death <- unique(df_death$census_id)
	
sampled_ids_death <- sample(ids_death, length(ids_death), replace = T)

sampled_dead <- data.frame(census_id = sampled_ids_death) %>%
			left_join(df_death, by = "census_id")
  
  #sample deaths

df_alive <- df %>% filter(death == 1)

ids_alive <- unique(df_alive$census_id)

sampled_ids_alive <- sample(ids_alive, length(ids_alive), replace = T)

sampled_alive <- data.frame(census_id = sampled_ids_alive) %>%
			left_join(df_alive, by = "census_id")
  
#sanity check that this is diff each time	
#mean(as.numeric(sampled_alive$dv_agedateadministered_yrs)) %>% print()  
	
data_sample <- rbind(sampled_dead, sampled_alive)
		
return(data_sample)
	
}


## paraellisation 

## iteration for manual loop #############################

set.seed(run)

run_date <- Sys.Date()
run_date <- gsub('-', '_', run_date)

x <- foreach(i = 1:500, .combine = "c") %dopar% {
  
  
bootstrapped_output <- sample_df(data_sample)


#run covid-19 and non-covid models

covid_outcomes <- coxph(as.formula(paste0(cox_surv_covid, " ~ ", cox_control_6)), 
                 data = bootstrapped_output,
                 weights = bootstrapped_output$weight)

noncovid_outcomes <- coxph(as.formula(paste0(cox_surv_noncovid, " ~ ", cox_control_6)), 
                 data = bootstrapped_output,
                 weights = bootstrapped_output$weight)


#extract coefs from each model
covid_exp <- tidy(covid_outcomes, exp = T, conf.int = T) %>%
							select(term, estimate, conf.low, conf.high) %>% 
							filter(term %in% comorbidities) %>% 
							rename(covid_val = estimate) %>%
              mutate(iteration = i)

covid_modeloutput <- rbind(covid_modeloutput, covid_exp)

non_covid_exp <- tidy(noncovid_outcomes, exp = T, conf.int = T) %>%
							select(term, estimate, conf.low, conf.high) %>% 
							filter(term %in% comorbidities) %>% 
							rename(non_covid_val = estimate) %>%
              mutate(iteration = i)
  
noncovid_modeloutput <- rbind(noncovid_modeloutput, non_covid_exp)


model_term <- left_join(covid_exp, non_covid_exp, by = "term")

#store hrs for covid and non-c
  
model_term <- model_term %>%
	mutate(diff = covid_val - non_covid_val) %>%
	select(term, diff)


diff_coef_it <- spread(data=model_term, key=term, value=diff)

bootstrap_iterations <- rbind(bootstrap_iterations, diff_coef_it)

write.csv(bootstrap_iterations, paste0("<path>", run, "_", run_date, ".csv"))

write.csv(covid_modeloutput, paste0("<path>", run, "_", run_date, ".csv"))

write.csv(noncovid_modeloutput, paste0("<path>", run, "_", run_date, ".csv"))

}










