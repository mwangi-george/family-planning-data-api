library(plumber)
options(scipen = 999)

pr(file = "time-series-analysis-api/plumber.R") %>% 
  pr_run(port = 9000)
