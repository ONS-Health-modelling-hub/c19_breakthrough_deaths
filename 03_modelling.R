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
#library(simPH)
#library(statip)

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

#to do - re-run with region, new data from yday
#round all counts 
#age spline data 
#re-run boostrapping with correct model

start_date = "2022-09-01"

source("<path>/functions.R")


#read in from memory and run in pre-prod

df <- readRDS("<path>/autumn22boosters_2023_06_08.RDS")

df <- df %>% 
    mutate(cause_death = case_when(covid_death == 1 & death == 1 ~ "Covid19death",
                                                                non_covid_death == 1 & death == 1 ~ "NonCovid19death", 
                                                                    death == 0 ~ "Alive"))

df <- df %>% 
  mutate(caltime = round(as.numeric(difftime(dose_date, start_date), units="days"), 1))

df_flag <- df %>% select(contains("_flag")) 
healthlist <- paste(dQuote(colnames(df_flag)), collapse = '+' )


# replace na
df <- df %>% 
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

df$dv_agedateadministered_yrs <- as.numeric(df$dv_agedateadministered_yrs)


#re-level all categories
df$sex <- as.factor(df$sex)
df$sex <- relevel(df$sex, ref = "Male")

df$bmi_group <- as.factor(df$bmi_group)
df$bmi_group <- relevel(df$bmi_group, ref = "Ideal")

df$health_in_general <- as.factor(df$health_in_general)
df$health_in_general <- relevel(df$health_in_general, ref = "Very good health")

df$disability <- as.factor(df$disability)
df$disability <- relevel(df$disability, ref = "No")

df$ethnicity <- as.factor(df$ethnicity)
df$ethnicity <- relevel(df$ethnicity, ref = "White British")

df$dv_agedateadministered_yrs <- as.numeric(df$dv_agedateadministered_yrs) 
#-----------
# Model arugements 
#-----------

cox_surv_covid <- 'Surv(time_at_risk, covid_death)'

cox_surv_noncovid <- 'Surv(time_at_risk, non_covid_death)'

cox_exposure <- ''

cox_control_1 <- 'ns(dv_agedateadministered_yrs, df=3, Boundary.knots=quantile(dv_agedateadministered_yrs, c(.05, .95))) + sex + ns(caltime, df=3, Boundary.knots=quantile(caltime, c(.05, .95)))'

cox_control_2 <- 'ns(dv_agedateadministered_yrs, df=3, Boundary.knots=quantile(dv_agedateadministered_yrs, c(.05, .95))) + sex + ns(caltime, df=3, Boundary.knots=quantile(caltime, c(.05, .95))) + ethnicity + region'

cox_control_3 <- paste(cox_control_2, '+ bmi_group')

cox_control_4 <- paste(cox_control_2, '+ disability')

cox_control_5 <- paste(cox_control_2, '+ bmi_group + disability')

cox_control_6 <- paste(cox_control_2, '+ MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag + leukolaba_gpes_flag + LiverCirrhosis_gpes_flag + Asthma_gpes_flag + PriorFractureOfHipWristSpineHumerus_gpes_flag + SevereMentalIllness_gpes_flag +  Epilepsy_gpes_flag + CysticFibrosisBronchiectasisAlveolitis_gpes_flag + HeartFailure_gpes_flag + LungOrOralCancer_gpes_flag + PulmonaryHypertensionOrFibrosis_gpes_flag + PeripheralVascularDisease_gpes_flag + Dementia_gpes_flag + DiabetesT2_gpes_flag + ChronicKidneyDisease_gpes_flag + DiabetesT1_gpes_flag + AtrialFibrillation_gpes_flag + schiz_gpes_flag + CancerOfBloodOrBoneMarrow_gpes_flag + Copd_gpes_flag + RheumatoidArthritisOrSle_gpes_flag + ParkinsonsDisease_gpes_flag + LearningDisabilityOrDownsSyndrome_gpes_flag + ThrombosisOrPulmonaryEmbolus_gpes_flag + StrokeOrTia_gpes_flag + immunosuppressants_gpes_flag + CongenitalHeartProblem_gpes_flag + CoronaryHeartDisease_gpes_flag')

#########################################################################
#cox_control_4 <- paste(cox_control_2, '+ health_in_general')

#cox_control_5 <- paste(cox_control_2, '+ disability')

###some gdppr vars with only one level?
#values_count <- sapply(lapply(df, unique), length)  # Identify variables with 1 value
#values_count   

control_list <- list(cox_control_1, cox_control_2, cox_control_3, cox_control_4, cox_control_5,
                                        cox_control_6)


##why is ethn not rinning#############

#test <-coxph(Surv(time_at_risk, covid_death) ~ ethnicity, df, weight = weight) 

#run moodel
coxmodel_group_covid <- estimate_cox(df = df, 
                                   surv = cox_surv_covid, 
                                   exposure = cox_exposure,
                                   control = control_list, 
                                   weight = "weight")

coxmodel_group_noncovid <- estimate_cox(df = df, 
                                   surv = cox_surv_noncovid, 
                                   exposure = cox_exposure,
                                   control = control_list, 
                                   weight = "weight")

#loop for model outputs covid and non-covid

outputs <- data.frame()

for (i in 1:length(control_list)){
  
  covid_model <- tidy(coxmodel_group_covid$model[[i]], exp = T, conf.int = T) %>% 
    mutate(model = i) %>%
    mutate(outcome = "covid")
  
   non_covid_model <- tidy(coxmodel_group_noncovid$model[[i]], exp = T, conf.int = T) %>% 
    mutate(model = i) %>%
    mutate(outcome = "non-covid")
  
   both <- rbind(covid_model, non_covid_model)
  
   outputs <- rbind(outputs, both)
  
}

outputs %>% print()

outputs_format <- outputs %>%
  mutate(Estimate = paste0(round(estimate, 2),
                                             " (", 
                                             round(conf.low, 2),
                                             " - ",
                                             round(conf.high, 2),
                                             ")")) %>%
    select(-p.value, -statistic, -conf.high, -conf.low, -statistic, -estimate, -std.error) %>%
    filter(!grepl('dv_agedateadministered_yrs|caltime', term))

outputs_format %>% print()


outputs_wide <- spread(outputs_format, outcome, Estimate)
outputs_wide %>% arrange(term) %>% print()


run_date <- Sys.Date()
run_date <- gsub('-', '_', run_date)

write.csv(outputs_wide, paste0("<path>", run_date, ".csv"))
write.csv(outputs, paste("<path>", run_date, ".csv"))

##########

##supplementary models for age + sex + cal time and condition ####################

cox_surv_covid <- 'Surv(time_at_risk, covid_death)'

cox_surv_noncovid <- 'Surv(time_at_risk, non_covid_death)'

cox_exposure <- ''

cox_control_2 <- 'ns(dv_agedateadministered_yrs, df=3, Boundary.knots=quantile(dv_agedateadministered_yrs, c(.05, .95))) + sex + ns(caltime, df=3, Boundary.knots=quantile(caltime, c(.05, .95))) + ethnicity + region'

exposure <- c("bmi_group",
               "health_in_general",
               "disability",
               "MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag",
               "leukolaba_gpes_flag",
                             "LiverCirrhosis_gpes_flag",
                                "Asthma_gpes_flag",
                                "PriorFractureOfHipWristSpineHumerus_gpes_flag",
                                "Diabetes_gpes_flag",
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

for (i in 1:length(exposure)){
  
  m1 <- paste0(cox_control_2, " + ", exposure)

}

coxmodel_group_covid_supp <- estimate_cox(df = df, 
                                   surv = cox_surv_covid, 
                                   exposure = cox_exposure,
                                   control = m1, 
                                   weight = "weight")

coxmodel_group_noncovid_supp <- estimate_cox(df = df, 
                                   surv = cox_surv_noncovid, 
                                   exposure = cox_exposure,
                                   control = m1, 
                                   weight = "weight")


outputs_supp <- data.frame()

for (i in 1:length(m1)){
  
  covid_model <- tidy(coxmodel_group_covid_supp$model[[i]], exp = T, conf.int = T) %>% 
    mutate(model = i) %>%
    mutate(outcome = "covid")
  
   non_covid_model <- tidy(coxmodel_group_noncovid_supp$model[[i]], exp = T, conf.int = T) %>% 
    mutate(model = i) %>%
    mutate(outcome = "non-covid")
  
   both <- rbind(covid_model, non_covid_model)
  
   outputs_supp <- rbind(outputs_supp, both)
  
}

outputs_supp %>% print()

outputs_format_supp <- outputs_supp %>%
  mutate(Estimate = paste0(round(estimate, 2),
                                             " (", 
                                             round(conf.low, 2),
                                             " - ",
                                             round(conf.high, 2),
                                             ")")) %>%
    select(-p.value, -statistic, -conf.high, -conf.low, -statistic, -estimate, -std.error) %>%
    filter(!grepl('dv_agedateadministered_yrs|caltime', term))

outputs_format_supp %>% print()


outputs_wide_supp <- spread(outputs_format_supp, outcome, Estimate)
outputs_wide_supp %>% arrange(term) %>% print()


write.csv(outputs_wide_supp, paste0("<path>", run_date, ".csv"))

##age spline ################################################################

age_list <- data.frame(dv_agedateadministered_yrs = rep(seq(50,100,1)))            
sex_list <- df %>% filter(!is.na(sex)) %>% expand(sex)

join_test <- merge(age_list, sex_list)
join_test$caltime <- mean(df$caltime)


#plot code - covid-19 #############################################################################

# fit the model to the pred_baseline table, extract the fit and se, calc the C.i
pred <- predict(coxmodel_group_covid$model[[1]], join_test, type='lp', se.fit=TRUE) 

pred_ci<- data.frame(fit = pred$fit,
                        se = pred$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

joinplot <- cbind(join_test, pred_ci)

#ref for age plot
reference <- joinplot %>%
  filter(dv_agedateadministered_yrs == 50, sex == "Male")

rb <- reference$fit

joinplot <- joinplot %>% 
  mutate(HR = exp(fit - rb),HRci1 = exp(ci1- rb), HRci2 = exp(ci2 -rb)) %>% 
  mutate(outcome = "covid")

covid_HR <- ggplot(data=joinplot, aes(x=dv_agedateadministered_yrs, y=HR, group=sex)) +
geom_ribbon(data=joinplot, aes(ymin=HRci2, ymax=HRci1, fill=factor(sex)), alpha=0.30, show.legend=FALSE)+ 
geom_line(aes(colour=sex)) +
theme(legend.title=element_blank()) 

plot(covid_HR)
#plot code - noncovid-19 #############################################################################

# fit the model to the pred_baseline table, extract the fit and se, calc the C.i
pred02 <- predict(coxmodel_group_noncovid$model[[1]], join_test, type='lp', se.fit=TRUE) 

pred_ci02<- data.frame(fit = pred02$fit,
                        se = pred02$se.fit) %>%
              mutate(ci1 = fit+ (1.96*se),
                    ci2 = fit - (1.96*se)) %>%
              select(fit, ci1, ci2)

joinplot02 <- cbind(join_test, pred_ci02)

#ref for age plot
reference02 <- joinplot02 %>%
  filter(dv_agedateadministered_yrs == 50, sex == "Male")

rb <- reference02$fit

joinplot02 <- joinplot02 %>% 
  mutate(HR = exp(fit - rb),HRci1 = exp(ci1- rb), HRci2 = exp(ci2 -rb)) %>% 
  mutate(outcome = "non-covid")

noncovid_HR <- ggplot(data=joinplot02, aes(x=dv_agedateadministered_yrs, y=HR, group=sex)) +
geom_ribbon(data=joinplot02, aes(ymin=HRci2, ymax=HRci1, fill=factor(sex)), alpha=0.30, show.legend=FALSE)+ 
geom_line(aes(colour=sex)) +
theme(legend.title=element_blank()) 

plot(noncovid_HR)

join_est <- rbind(joinplot, joinplot02)

all <- ggplot(data=join_est, aes(x=dv_agedateadministered_yrs, y=HR, group=outcome)) +
  geom_ribbon(data=join_est, aes(ymin=HRci2, ymax=HRci1, fill=factor(outcome)), alpha=0.30, show.legend=FALSE)+ 
  geom_line(aes(colour=outcome)) +
  facet_wrap(~sex) +
  theme(legend.title=element_blank()) 

plot(all)
ggsave(paste0("<path>age_plot_", run_date, ".jpg"))


write.csv(join_est, paste0("<path>/age_spline_est_", run_date, ".csv"))


join_est %>% filter(dv_agedateadministered_yrs == 50 & sex == "Male"| dv_agedateadministered_yrs == 80 & sex == "Male") %>% print()



