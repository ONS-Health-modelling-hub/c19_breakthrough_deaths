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
library(data.table)

source("<path>/functions.R")

exposure <- c("MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea_gpes_flag1",
               "leukolaba_gpes_flag1",
                             "LiverCirrhosis_gpes_flag1",
                                "Asthma_gpes_flag1",
                                "PriorFractureOfHipWristSpineHumerus_gpes_flag1",
                                "Diabetes_gpes_flag1",
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


joined_df <- data.frame()

for (i in 1:2){
  
boot_val <- read.csv(paste0("<path>", i, "_", "2023_06_08", ".csv"))

joined_df <- rbind(joined_df, boot_val)
  
}



std_dev <- joined_df %>% select(-X) %>% summarise_if(is.numeric, sd)

std_dev %>% print()

data_long <- gather(std_dev, condition, value, Asthma_gpes_flag1:ThrombosisOrPulmonaryEmbolus_gpes_flag1, factor_key=TRUE)

data_long %>% print()

##read in model outcomes read in
model_estimates <- read.csv(paste0("<path>/model_est_raw_ 2023_06_08 .csv"))

model_estimates_8 <- model_estimates %>%
  filter(model == 6) %>% 
  select(term, estimate, outcome) %>% 
  filter(term %in% exposure)

model_estimates_8_wide <- spread(model_estimates_8, outcome, estimate)


join_est <- left_join(model_estimates_8_wide, data_long, by = c("term" = "condition")) %>%
  rename(standard_dev_H1_H2_boot = value,
        non_covid = 'non-covid') %>% 
  mutate(z_value = (covid - non_covid)/standard_dev_H1_H2_boot) %>%
  mutate(significance = ifelse(z_value > 1.96 | z_value < -1.96, 1, 0))

join_est %>% arrange(significance) %>% print()
  
write.csv(join_est, paste0("<path>/bootstrapping_bothmodels_2023_06_09.csv"))

## code to supress counts #############################################################################
  
sdc_fun <- function(data, n_var, perc_var) {
  
  sdc_data <- data %>%
    mutate(across(.cols = all_of(n_var),
                  .fns = ~ ifelse(. < 10,
                                  NA_real_,
                                  round(. / 5) * 5)))
  
  if (!missing(perc_var)) {
    
    sdc_data <- sdc_data %>%
      mutate(across(.cols = all_of(perc_var),
                    .fns = ~ ifelse(is.na(.data[[n_var]]),
                                    NA_real_,
                                    round(. * 100, 2))))
    
  }
  
  return(sdc_data)  
  
}




#demographic table #######################################################################################
  
demogs <- read.csv(paste0("<path>/demographics2023_06_08.csv"))

  
deomogs_round <- sdc_fun(data = demogs, n_var = c("totalsample","Covid19death", "NonCovid19death")) %>% 
  select(-X) %>%
  rename(Level = group, 
        Totalpopulation = totalsample, 
        Group = domain, 
        COVID19death = Covid19death, 
        NonCOVID19death = NonCovid19death)

write.csv(deomogs_round, paste0("<path>/demographics_round_2023_06_09.csv"))

#sample flow counts table
  

sample_flow <- read.csv(paste0("cen_dth_gps/autumn_boosters22/outputs/sample_flow2023_06_08.csv"))

  
sample_flow_round <- sdc_fun(data = sample_flow, n_var = c("count")) %>% 
  select(-X) 
  
write.csv(sample_flow_round, paste0("<path>/sample_flow_round_2023_06_09.csv"))

##quantiles of estimates ####################################################################################

quantile(joined_df$Asthma_gpes_flag1, probs=c(0.025, 0.975))

col_numeric <- which( sapply(joined_df, is.numeric ) )  

quant <- setDT(joined_df)[, lapply( .SD, quantile, probs = c(0.025, 0.975), na.rm = TRUE ), .SDcols = col_numeric ]

quant <- quant %>% select(-X)

write.csv(quant, paste0("<path>/quant_est_2023_06_09.csv"))

