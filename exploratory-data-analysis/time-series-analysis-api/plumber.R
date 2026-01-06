
# Main Plumber Router

# Load required packages ===============================
library(plumber)

source(here::here("time-series-analysis-api/utils.R"))

#* @apiTitle Family Planning Time Series Analysis API
#* @apiDescription This API provides a set of endpoints for analyzing family planning consumption & service data from DHIS2


#* Setup a logger to log all incoming requests
#* @filter api_logger
function(req){
  cat(
      as.character(Sys.time()), "-",
      req$REQUEST_METHOD, req$PATH_INFO, "-",
      req$HTTP_USER_AGENT, "@", req$REMOTE_ADDR, "\n"
      )
    # pass control off to the next handler in the chain. 
    forward()
}

# Overwrite the default serializer to return unboxed JSON 
#* @plumber
function(pr) {
    pr %>% pr_set_serializer(serializer_unboxed_json())
}

#* Check API status
#* @get /api/v1/health
function() {
    return(
        list(
            status = "OK"
        )
    )
}

#* Plot time series plots
#* @param analytic:string Name of the family planning product to analyze
#* @param method:string One of: "Consumption", "Service"
#* @param org_unit:string Org unit being to analyze
#* @param start_date:string Start date in format YYYY-MM-DD.
#* @param end_date:string End date in format YYYY-MM-DD.
#* @param plot_type:string One of: "time_series", "seasonal_diagnostics", "stl_diagnostics".
#* @param visualize_all_methods:logical If TRUE, groups by method and plots both service and consumption data
#* @serializer htmlwidget
#* @get /api/v1/analytics/time-series
function(
    analytic, 
    method = "Consumption",
    org_unit = "Kenya", 
    start_date = "2020-01-01",
    end_date = "2025-12-01",
    plot_type = "time_series",
    visualize_all_methods = FALSE
    ) {
  
  generate_time_series_plot(
    analytic_col = analytic,
    method_col = method,
    org_unit_col = org_unit,
    start_date = start_date,
    end_date = end_date,
    plot_type = plot_type,
    visualize_all_methods = visualize_all_methods
  )
}


#* Plot Anomaly Detection plots
#* @param analytic:string Name of the family planning product to analyze
#* @param method:string One of: "Consumption", "Service"
#* @param org_unit:string Org unit being to analyze
#* @param start_date:string Start date in format YYYY-MM-DD.
#* @param end_date:string End date in format YYYY-MM-DD.
#* @param plot_type:string One of: "anomalies", "anomalies_cleaned".
#* @param normal_range_width:numeric Value between 0 and 1. Controls the width of the "normal" range. Lower values are more conservative while higher values are less prone to incorrectly classifying "normal" observations.
#* @param max_anomalies:numeric Value between 0 and 1. The maximum percent of anomalies permitted to be identified.
#* @param visualize_all_methods:logical If TRUE, groups by method and plots both service and consumption data
#* @serializer htmlwidget
#* @get /api/v1/analytics/anomaly-detection
function(
    analytic, 
    method = "Consumption",
    org_unit = "Kenya", 
    start_date = "2020-01-01",
    end_date = "2025-12-01",
    plot_type = "anomalies",
    normal_range_width = 0.15,
    max_anomalies = 0.3,
    visualize_all_methods = FALSE
    ) {
  
  normal_range_width_num <- as.numeric(normal_range_width)
  max_anomalies_num <- as.numeric(max_anomalies)
  
  generate_anomalies_plot(
    analytic_col = analytic,
    method_col = method,
    org_unit_col = org_unit,
    start_date = start_date,
    end_date = end_date,
    plot_type = plot_type,
    normal_range_width = normal_range_width_num,
    max_anomalies = max_anomalies_num,
    visualize_all_methods = visualize_all_methods
  )
}

