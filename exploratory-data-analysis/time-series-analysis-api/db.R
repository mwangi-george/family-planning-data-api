library(DBI)
library(RPostgres)


config <- config::get(config = "production")

# Connect to postgres database
db_con <- dbConnect(
  drv = RPostgres::Postgres(),
  dbname = config$DB_NAME,
  host = config$HOST, 
  port = config$PORT,
  user = config$USERNAME,
  password = config$PASSWORD
)
