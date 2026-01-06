
source(here::here("time-series-analysis-api/db.R"))

# Fetch summary data from db - county & national level
fp_data <- dbGetQuery(
  conn = db_con,
  statement = "select * from cleaned_fp_summary_data;"
)
