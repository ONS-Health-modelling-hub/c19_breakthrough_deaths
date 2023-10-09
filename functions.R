estimate_cox <- function(df, surv, exposure, weight=NULL, controls, save_model=T){
  
  # estimates models
  # Put check in for weights existing
  if (!is.null(weight)){
  wgt = df[[weight]]
  }else{
    wgt=rep(1, nrow(df))
  }
  
  models <- lapply(controls, function(control){
      cat(paste0("Fit model:", control, "\n \n"))
      m <- coxph(as.formula(paste0(surv, " ~", exposure, "+", control)), 
                 data = df,
                 weights = wgt)
      return(m)
  })
 
  # get HR and CI in dataframe
  df_hr_models <- purrr::map_dfr(1:length(models), function(i){
    
    df_hr_model <- broom::tidy(models[[i]], exp=TRUE) %>%
      mutate(model=i)

    hr_ci <- data.frame(confint.default(models[[i]]))
    colnames(hr_ci) <- c("conf.low", "conf.high")
    hr_ci <- hr_ci %>%
      mutate(conf.low = exp(conf.low),
             conf.high = exp(conf.high))
      
    df_hr_model <- bind_cols(df_hr_model, hr_ci) %>%
      filter(grepl(exposure, term)) %>%
      mutate(term = gsub(exposure, "", term))


    return(df_hr_model)
  
  })
  
  conc <- sapply(models, function(m){summary(m)$concordance[[1]]})
    
  diagnostics <- data.frame(model=1:length(models), concordance = conc)
 
  ## Output
  # create empty list to store results
  output <- list()
  
  # return the models
  if(save_model == TRUE){
  output$models <- models 
  }
  # return the HR
  output$hr <-  df_hr_models
  
  # diagnostics
  output$diagnostics <- diagnostics

  return(output)
}