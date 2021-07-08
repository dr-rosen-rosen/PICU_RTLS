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
#library(TraMineR)
#library(data.table)
library(patchwork)
debuggingState(on=FALSE)
# start ve with: source python3/bin/activate in project folder
Sys.setenv(R_CONFIG_ACTIVE = "pilot_study_RTLS_030819_db") # 'default')#
config <- config::get()
Sys.setenv(RETICULATE_PYTHON = config$py_version)
reticulate::source_python('1_funcs.py')
source(here('1_funcs.R'), echo = TRUE)
source(here('2a_connect.R'), echo = TRUE)

########## Loads in automated csv files
csv_to_db(
  db_name = config$db_name,
  db_loc = config$db_loc,
  in_path = config$in_path
  )

########## Updates receiver location codes
rcvr_dscrp_to_loc_code(
  db_name = config$db_name,
  db_loc = config$db_loc,
  rcvr_recode_file = config$rcvr_recode_file,
  rcvr_recode_file_loc = config$rcvr_recode_file_loc
  )

# test variables
#badges <- c('451616','404057')
#badges <- 'all'
#badges <- c(404057,451616,408427,451613,451616,451635,451795,474612)
strt <- as.POSIXct("2019-07-28")#lubridate::ymd("2019-07-28")
stp <- as.POSIXct("2019-08-12")#lubridate::ymd("2019-08-12")

### reports

x <- get_weekly_report(
  anchor_date = lubridate::today(),
  look_back_days = 120, # 
  db_name = config$db_name,
  db_loc = config$db_loc,
  target_badges = get_active_badges(config$badge_file),
  weekly_report_dir = config$weekly_report_dir
  )


create_FB_reports(
  target_badges = get_active_badges(config$badge_file),
  strt = config$FB_report_start,
  stp = config$FB_report_stop,
  FB_report_dir = config$FB_report_dir
)

# load some badge data
x <- get_RTLS_data(
  badges = get_active_badges(config$badge_file),#'all',#badges, 
  strt = config$FB_report_start,#'all',#strt, 
  stp = config$FB_report_stop#'all'#stp
  )

# code into location categories
y <- loc_code_badge_data(
  badge_data = x,
  db_name = config$db_name,
  db_loc = config$db_loc
  )
# apply data cleaning rules
z <- apply_rules(
  df = y,
  rule_1_thresh = config$rule_1_thresh,
  rule_2_thresh = config$rule_2_thresh,
  rule_2_locs = config$rule_2_locs
  )
#z <- z %>% filter(between(Time_In,as.POSIXct(ydm("2019-28-7")),as.POSIXct(ydm("2019-12-8"))))

zz <- make_interval_metrics(df = z)

zzz <- make_timeseries_df(
  df = z[which(z$Badge == 308759),],#, %>% filter(between(Time_In,min(z$Time_In),min(z$Time_In)+days(14))),
  f = 'm'#'min' # Using a minute for this seems to overinflate 'transition' ... i think because there are many small hits that all get rounded up... 'S' takes forever though
)

make_timeseries_df_for_dummies(z[which(z$Badge == 308759),])

zzz_par <- make_timeseries_df_PAR(
  #fun = ts_it_PAR,
  df = z,
  f = 'S',
  num_processes = NULL
)

ind_area_norm <- make_area_plot(
  df = make_timeseries_df(df = z[which(z$Badge == 451616), ],f = 'min'),
  perc = TRUE, #if true, will create porportional chart ,if false will do raw
  badge = 451616 # if NULL this assumes a summary plot; if an int it will use that in plot titles
)
show(ind_area_norm)

col_area_norm <- make_area_plot(
  df = zzz,#make_timeseries_df(df = z[which(z$Badge == 403606), ],f = 'min'),
  perc = TRUE, #if true, will create porportional chart ,if false will do raw
  badge = NULL#403606 # if NULL this assumes a summary plot; if an int it will use that in plot titles
)
show(col_area_norm)

# test <- make_seqdef_data(
#   df = z[which(z$Badge == 474612), ],
#   start = strt,
#   stop = stp,
#   f = 'min'
# )
# is.nan.data.frame <- function(x)
#   do.call(cbind, lapply(x, is.nan))
# test2[is.nan(test2)] <- NA
# loc_seq <- TraMineR::seqdef(test,id='auto')

overall_bar <- make_overall_bar(
  df = z, #%>% filter(between(Time_In,min(z$Time_In),min(z$Time_In)+days(14))),
  badge = 403606
)
show(overall_bar)

overall_area_raw <- make_area_plot(
  df = zzz,
  perc = FALSE, #if true, will create porportional chart ,if false will do raw
  badge = NULL # if NULL this assumes a summary plot; if an int it will use that in plot titles

)
show(overall_area_raw)

overall_area_perc <- make_area_plot(
  df = zzz,
  perc = TRUE, #if true, will create porportional chart ,if false will do raw
  badge = NULL # if NULL this assumes a summary plot; if an int it will use that in plot titles

)
show(overall_area_perc)

badge_area_raw <- make_area_plot(
  df = zzz,
  perc = FALSE, #if true, will create porportional chart ,if false will do raw
  badge = 474612 # if NULL this assumes a summary plot; if an int it will use that in plot titles
)
show(badge_area_raw)

badge_area_perc <- make_area_plot(
  df = zzz,
  perc = TRUE, #if true, will create porportional chart ,if false will do raw
  badge = 474612 # if NULL this assumes a summary plot; if an int it will use that in plot titles
)
show(badge_area_perc)

#(overall_area_raw | overall_area_perc) / (badge_area_raw | badge_area_perc)  + plot_layout(guides = 'collect')

overall_bar | (col_area_norm / ind_area_norm) + plot_layout(guides = 'collect')

### make _workflow diagram?
### busiest day? Day with most pt room time?
### make_network
### put into word doc... officer... create template
### profit.
