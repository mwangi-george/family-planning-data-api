suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(glue)
  library(timetk)
})

# Ensure data is available (fp_data)
source(here::here("time-series-analysis-api/prepare_data.R"))


generate_time_series_plot <- function(
    analytic_col, 
    method_col = "Consumption", 
    org_unit_col = "Kenya",
    start_date = "2020-01-01",
    end_date = "2025-12-01",
    visualize_all_methods = TRUE,
    plot_type = "time_series"   # time_series | seasonal_diagnostics | stl_diagnostics
    ) {
  tryCatch(
    expr = {
      base_filtered_df <- fp_data %>% 
        filter(
          analytic == analytic_col, 
          org_unit == org_unit_col,
          period >= start_date,
          period <= end_date,
        )
      
      base_filtered_df %>% glimpse()
      
      if (visualize_all_methods) {
        df <- base_filtered_df %>% group_by(method)
      } else {
        df <- base_filtered_df %>% filter(method == method_col)
      }
      
      if (nrow(df) < 1) {
        return(glue("{analytic_col} has insufficient data to generate plot"))
      }
      
      df %>% glimpse()
      # Determine if we should facet based on the grouping
      facet_var <- if (visualize_all_methods) "method" else NULL
      
      if (plot_type == "time_series") {
        title_text <- if (visualize_all_methods) {
          glue::glue("Time Series Plot for {analytic_col}")
        } else {
          glue::glue("Time Series Plot for {analytic_col}, {method_col}")
        }
        df %>% 
          plot_time_series(
            .date_var = period,
            .value = value,
            .smooth = FALSE,
            .facet_vars = !!facet_var,
            .title = title_text,
            .x_lab = "Date",
            .y_lab = "Value"
          )
      } else if (plot_type == "seasonal_diagnostics") {
        title_text <- if (visualize_all_methods) {
          glue::glue("Seasonal Diagnostics Plot for {analytic_col}")
        } else {
          glue::glue("Seasonal Diagnostics Plot for {analytic_col}, {method_col}")
        }
        df %>% 
          plot_seasonal_diagnostics(
            .date_var = period,
            .value = value,
            .facet_vars = !!facet_var,
            .title = title_text,
            .x_lab = "Date",
            .y_lab = "Value"
          )
      } else if (plot_type == "stl_diagnostics") {
        title_text <- if (visualize_all_methods) {
          glue::glue("Seasonal-Trend decomposition Plot for {analytic_col}")
        } else {
          glue::glue("Seasonal-Trend decomposition Plot for {analytic_col}, {method_col}")
        }
        df %>% 
          plot_stl_diagnostics(
            .date_var = period,
            .value = value,
            .facet_vars = !!facet_var,
            .title = title_text,
            .x_lab = "Date",
            .y_lab = "Value"
          )
      } else {
        return("Invalid time series plot type")
      }
    },
    error = function(e) {
      print(e$message)
      return(glue("An error occurred while generating plot: {e$message}"))
    }
  )
}


generate_anomalies_plot <- function(
    analytic_col, 
    method_col = "Consumption", 
    org_unit_col = "Kenya",
    start_date = "2020-01-01",
    end_date = "2025-12-01",
    normal_range_width = 0.15,
    max_anomalies = 0.3,
    visualize_all_methods = TRUE,
    plot_type = "anomalies"  # anomalies | anomalies_cleaned
) {
  tryCatch(
    expr = {
      base_filtered_df <- fp_data %>% 
        filter(
          analytic == analytic_col, 
          org_unit == org_unit_col,
          period >= start_date,
          period <= end_date,
        )
      
      # Handle Grouping/Filtering for Methods
      if (visualize_all_methods) {
        df <- base_filtered_df %>% group_by(method)
        facet_var <- "method"
      } else {
        df <- base_filtered_df %>% filter(method == .env$method_col)
        facet_var <- NULL
      }
      
      if (nrow(df) < 1) {
        return(glue("{analytic_col} has insufficient data to generate anomalies plot"))
      }
      
      df %>% glimpse()
      
      title_text <- if (visualize_all_methods) {
        glue::glue("Anomalies Plot for {analytic_col}")
      } else {
        glue::glue("Anomalies Plot for {analytic_col}, {method_col}")
      }
      # Determine if we should facet based on the grouping
      facet_var <- if (visualize_all_methods) "method" else NULL
      
      anomalized_df <- df %>%  
        anomalize(
          .date_var = period,
          .value = value, 
          .max_anomalies = max_anomalies,
          .iqr_alpha = normal_range_width
          ) 
      
      if (plot_type == "anomalies") {
        anomalized_df %>% 
          plot_anomalies(
            .date_var = period,
            # .facet_vars = if (!is.null(facet_var)) !!rlang::sym(facet_var) else NULL,
            .title = title_text,
            .x_lab = "Date",
            .y_lab = "Value"
        )
      } else if (plot_type == "anomalies_cleaned") {
        anomalized_df %>% 
          plot_anomalies_cleaned(
            .date_var = period,
            .title = title_text,
            # .facet_vars = if (!is.null(facet_var)) !!rlang::sym(facet_var) else NULL,
            .x_lab = "Date",
            .y_lab = "Value"
        )
      } else {
        return("Invalid anomaly plot type")
      }
    },
    error = function(e) {
      print(e$message)
      return(glue("An error occurred while generating anomalies plot: {e$message}"))
    }
  )
}



