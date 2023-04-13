###############################################################################################
###############################################################################################
#####################   Main File for Running Feedback Reports for RTLS data
#####################
###############################################################################################
###############################################################################################

library(here)
library(config)
library(reticulate)
library(lubridate)
library(tidyverse)
library(patchwork)
debuggingState(on=FALSE)
# start ve with: source python3/bin/activate in project folder
Sys.setenv(R_CONFIG_ACTIVE = 'default')#"pilot_study_RTLS_030819_db") # 
config <- config::get()
# Sys.setenv(RETICULATE_PYTHON = config$py_version)
# reticulate::source_python('1_funcs_pg.py')
source(here('1_funcs.R'), echo = TRUE)

# read rtls into db
# top_folder <- '/Users/mrosen44/Johns Hopkins/Salar Khaleghzadegan - Project_CollectiveAllostaticLoad/PICU Data Collection'
csv_to_db(top_folder,
          con = get_connection(
            db_name = config$db_name,
            db_u = config$db_u,
            db_pw = config$db_pw
          ))

tasks_df <- get_task_lists(
  data_dir = 'data',
  fname = 'PICU_Device_Assignment.xlsx'
)

# x <- get_rtls_metrics(tasks_df[which(tasks_df$shift_day == 'Shift_97'),])
x <- get_rtls_metrics(tasks_df)
hist(x$entropy)
hist(log(x$fano_factor))
hist(log(x$prop_time_in_pt_rm))
hist(log(x$min_in_pt_rm))
scatterplot3d::scatterplot3d(x$entropy,x$fano_factor,log(x$min_in_pt_rm))


# 
# t <- ymd_hms('2020-10-16 07:12:24')
# t <- lubridate::floor_date(t,unit = 'hours')




######### Automated data flow from email to postgres
#########

csv_to_db_pg(
  tmp_csv_path = config$tmp_csv_path, 
  archive_csv_path = config$archive_csv_path,
  db_u = config$db_u, 
  db_pw = config$db_pw) 


######### Pulling data for feedback reports and analysis
#########

site <- c('jhh','bmc') # 'jhh','bmc'
strt <-  lubridate::ymd('2023-01-01')#config$FB_report_start,#lubridate::ymd('2022-02-27'), 
stp <- lubridate::ymd('2023-04-01')#config$FB_report_stop,#lubridate::ymd('2022-03-01'), 
data_for_fb_df  <- get_and_locCode_RTLS_data_pg(
  badges = getActiveBadges(config$badge_file), #unique(bayview_active_badges$RTLS_ID), 
  strt = strt, #lubridate::ymd('2022-01-01'),#config$FB_report_start,#lubridate::ymd('2022-02-27'), 
  stp = stp, #lubridate::ymd('2022-7-01'),#config$FB_report_stop,#lubridate::ymd('2022-03-01'), 
  sites = site, #c('bmc'), # 'jhh','bmc'
  use_rules = TRUE # this is currently commented out in the function
)


# Clean timelines
data_for_fb_df <- data_for_fb_df %>%
  mutate(
    receiver_recode = as.character(receiver_recode)
  )
data_for_fb_df <- data_for_fb_df %>%
  filter(date(time_in) < date(time_out)) %>%
  apply(1,split_days) %>%
  dplyr::bind_rows() %>%
  mutate(
    time_in = as.POSIXct(time_in,origin = "1970-01-01", tz = "America/New_York"),
    time_out = as.POSIXct(time_out,origin = "1970-01-01", tz = "America/New_York")
  ) %>%
  mutate(
    time_in = if_else(is.na(time_in),
                      as.POSIXct(paste(as.character(lubridate::date(time_out)),"00:00:00"), format = "%Y-%m-%d %H:%M:%S"),
                      time_in),
    time_out = if_else(is.na(time_out),
                       as.POSIXct(paste(as.character(lubridate::date(time_in)),"23:59:59"), format = "%Y-%m-%d %H:%M:%S"),
                       time_out),
  ) %>%
  mutate(
    duration = as.numeric(difftime(time_out,time_in, units = "mins"))
  ) %>%
  full_join(data_for_fb_df[which(date(data_for_fb_df$time_in) == date(data_for_fb_df$time_out)),]) %>%
  filter(if_any(everything(), ~ !is.na(.)))







