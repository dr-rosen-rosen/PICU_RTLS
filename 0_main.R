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
Sys.setenv(RETICULATE_PYTHON = config$py_version)
# reticulate::source_python('1_funcs.py')
reticulate::source_python('1_funcs_pg.py')
source(here('1_funcs.R'), echo = TRUE)

######### Automated data flow from email to postgres
#########

get_files_from_outlk(
  outlk_sess = Microsoft365R::get_business_outlook(),
  n = 10 # Number of emails to pull at once. Chokes with >10; for a full week it's 16 files w/ data and battery reports
)

csv_to_db_pg(
  tmp_csv_path = config$tmp_csv_path, 
  archive_csv_path = config$archive_csv_path,
  db_u = config$db_u, 
  db_pw = config$db_pw) 

get_weekly_report_pg(
  anchor_date = lubridate::today(),
  look_back_days = 8,
  db_u = config$db_u,
  db_pw = config$db_pw,
  target_badges = getActiveBadges(config$badge_file),
  weekly_report_dir = config$weekly_report_dir)
beepr::beep(sound = 1)

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

# Check for same badges with data at both sites on same day
data_for_fb_df %>%
  mutate(date = date(time_in)) %>%
  dplyr::select(site,badge,date) %>%
  distinct() %>%
  group_by(badge,date) %>%
  summarise(dupe = n()>1) %>%
  filter(dupe == TRUE)
write_csv(df,'duplicated_badges.csv')

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

######### Save Individual FB reports
#########

create_FB_reports(
  df = data_for_fb_df,
  FB_report_dir = config$FB_report_dir,
  save_badge_timeline = TRUE,
  min_hours_for_fb = 40*4*3 #config$min_hours_for_fb
)

######### Save data for analysis
#########

data_for_fb_df$receiver_recode <- factor(data_for_fb_df$receiver_recode,
                             levels = c('Patient room','MD Workroom','Ward Hall','Education','Supply and admin','Transit','OTHER/UNKNOWN','Family waiting space'))
data_for_fb_df <- data_for_fb_df %>% transform(
  receiver_recode=plyr::revalue(receiver_recode,c("OTHER/UNKNOWN"="Other",'Family waiting space'='Family space')))

# create summary by badge, day, and location
data_for_fb_df <- data_for_fb_df %>%
  mutate(date = date(time_in)) %>%
  dplyr::select(date,badge,receiver_recode, duration) %>%
  group_by(date, badge,receiver_recode) %>%
  summarize(
    duration = sum(duration, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(badge,date) %>%
  mutate(perc = duration / sum(duration)) %>%
  drop_na(perc)
# save daily summary
data_for_fb_df %>%
  write_csv(., paste0('all_',site,'_dataRun_',today(),'.csv'))
  
data_for_fb_df %>%
  # mutate(date = date(time_in)) %>%
  group_by(date, receiver_recode) %>%
  summarize(
    avg_perc = mean(perc, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(receiver_recode == 'Patient room') %>%
  group_by( yw = paste( year(date), week(date))) %>%
  mutate(date = min(date), avg_perc = mean(avg_perc)) %>%
  ggplot(aes(x=date,y=avg_perc)) + geom_line() +
  geom_vline(xintercept = lubridate::ymd("2020-11-19"), color = 'green') +
  geom_vline(xintercept = lubridate::ymd("2021-02-23"), color = 'red') +
  geom_vline(xintercept = lubridate::ymd("2021-05-11"), color = 'yellow') +
  geom_vline(xintercept = lubridate::ymd("2021-07-07"), color = 'green') +
  geom_vline(xintercept = lubridate::ymd("2021-08-04"), color = 'yellow') +
  geom_vline(xintercept = lubridate::ymd("2021-12-29"), color = 'red') +
  geom_vline(xintercept = lubridate::ymd("2022-01-05"), color = 'purple') +
  ggthemes::theme_tufte()
  
  
# write_csv(data_roll_up, 'dailySumDataJulyToDecember2021.csv')



make_overall_bar(df = data_for_fb_df, badge = 524787)

### get list of bayview badges we don't have data for:

active_badges <- getActiveBadges(config$badge_file)
bayview_active_badges <- readxl::read_excel('/Users/mrosen44/Johns Hopkins University/Amanda Bertram - Graduate Medical Training Laboratory/RTLS BADGES 2021-2022/Badge Numbers 2021.xlsx') %>%
  filter(CAMPUS == 'Bayview') %>%
  rename(c('RTLS_ID' = 'RTLS BADGE ID')) %>%
  filter(RTLS_ID %in% active_badges)


########## Updates receiver location codes

#### This is sqlite version. 
# rcvr_dscrp_to_loc_code(
#   db_name = config$db_name,
#   db_loc = config$db_loc,
#   rcvr_recode_file = config$rcvr_recode_file,
#   rcvr_recode_file_loc = config$rcvr_recode_file_loc,
#   print_new_recievers = TRUE
#   )




# test variables
#badges <- c('451616','404057')
#badges <- 'all'
#badges <- c(404057,451616,408427,451613,451616,451635,451795,474612)
strt <- as.POSIXct("2019-07-28")#lubridate::ymd("2019-07-28")
stp <- as.POSIXct("2019-08-12")#lubridate::ymd("2019-08-12")

### reports

x <- get_weekly_report(
  anchor_date = lubridate::today(),
  look_back_days = 8, #
  db_name = config$db_name,
  db_loc = config$db_loc,
  target_badges = getActiveBadges(config$badge_file),
  weekly_report_dir = config$weekly_report_dir
  )

test <- getActiveBadges(config$badge_file)
audit_active_badges(
  con = con,
  active_badges = getActiveBadges(config$badge_file)
)
create_FB_reports(
  target_badges = getActiveBadges(config$badge_file),
  strt = config$FB_report_start,
  stp = config$FB_report_stop,
  FB_report_dir = config$FB_report_dir,
  save_badge_timeline = FALSE
)

# load some badge data
x <- get_RTLS_data(
  badges = '526896',  #getActiveBadges(config$badge_file),#'all',#badges,
  strt = ymd("2021-10-20"),#config$FB_report_start,,#config$FB_report_start,#'all',#strt,
  stp = ymd("2021-10-22"), #config$FB_report_stop #config$FB_report_stop#'all'#stp
  con = get_connection(
    db_name = paste0('rtls_','bmc'),
    db_u = config$db_u,
    db_pw = config$db_pw)
  )

# code into location categories
y <- loc_code_badge_data_pg(
  badge_data = x,
  db_u = config$db_u,
  db_pw = config$db_pw,
  site = 'bmc'
  )
# apply data cleaning rules
z <- apply_rules(
  df = y,
  rule_1_thresh = config$rule_1_thresh,
  rule_2_thresh = config$rule_2_thresh,
  rule_2_locs = config$rule_2_locs
  )

# Build network viz
net_data <- prep_net_data(
  df = y
    )
# library(network)
# rtls_network <- network(
#   net_data$edges,
#   #vertex.attr = net_data$nodes,
#   matrix.type = "edgelist",
#   ignore.eval = FALSE)
# plot(rtls_network, vertex.cex = 3)
# detach(package:network)

library(igraph)

rtls_network <- igraph::graph_from_data_frame(
  d = net_data$edges,
  vertices = net_data$nodes,
  directed = TRUE
)
plot(rtls_network,
     layout = layout_with_graphopt)

library(visNetwork)

visNetwork(net_data$nodes,net_data$edges)

library(networkD3)
forceNetwork(Links = net_data$edges,
             Nodes = net_data$nodes,
             Source = "from",
             Target = "to",
             NodeID = "description",
             Group = "type",
             Value = "weight",
             Nodesize = "duration",
             opacity = 1,
             fontSize = 16,
             zoom = TRUE,
             legend = TRUE)

write_csv(net_data$nodes,path = '522400.csv')


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


################################################################
################################################################
############ Manual location updates
############
################################################################
################################################################

bmc_rec_df


################################################################
################################################################
############ Scipts for cleaning up table and column names
############
################################################################
################################################################

con <- get_connection(
  db_name = paste0('rtls_','jhh'),
  db_u = config$db_u,
  db_pw = config$db_pw)
tables <- dbListTables(con)
tables <- tables[tables != 'rtls_receivers']
tables <- tables[tables != 'RTLS_Receivers']
for (t in tables) {
  print(t)
  if (startsWith(t,"T")) {
    update_stmt <- paste0("ALTER TABLE IF EXISTS \"",t,"\" RENAME TO ",tolower(t),";")
    print(stmt)
    res <- DBI::dbExecute(con, update_stmt)
    print(res)
  }
}

for (t in tables) {
  print(t)
  if (startsWith(t,"table_")) {
    update_stmt <- paste0("ALTER TABLE IF EXISTS ",t," RENAME COLUMN \"Receiver\" TO receiver;")
    print(stmt)
    res <- DBI::dbExecute(con, update_stmt)
    print(res)
    update_stmt <- paste0("ALTER TABLE IF EXISTS ",t," RENAME COLUMN \"Time_In\" TO time_in;")
    print(stmt)
    res <- DBI::dbExecute(con, update_stmt)
    print(res)
    update_stmt <- paste0("ALTER TABLE IF EXISTS ",t," RENAME COLUMN \"Time_Out\" TO time_out;")
    print(stmt)
    res <- DBI::dbExecute(con, update_stmt)
    print(res)
  }
}


################################################################
################################################################
############ Scipts for pulling all old sqlite data into PG db
############
################################################################
################################################################


# Get sqlite connection
sqlite_con <- get_sqlite_con(
  db_loc = config$db_loc,
  db_name = config$db_name
)
# Get pg con
pg_con <- get_connection(
  db_name = paste0('rtls_','jhh'),
  db_u = config$db_u,
  db_pw = config$db_pw)

sqlt_tbls <- DBI::dbListTables(sqlite_con)

for (t in sqlt_tbls){
  print(t)
  if(!(grepl("RTLS",t))){
    # read data from table... set column names to lowercase
    badge_data <- sqlite_con %>%
      tbl(t) %>%
      collect() %>%
      rename_all(tolower) %>%
      mutate(across(c('time_in','time_out'), lubridate::ymd_hms))
    print(nrow(badge_data))
    badgeNum <- stringr::str_split(t,'_')[[1]][2]
    pgTable <- paste0('table_',badgeNum)
    if (pg_con %>% DBI::dbExistsTable(conn = pg_con, name = pgTable)) {
      DBI::dbAppendTable(conn = pg_con, name = pgTable, 
                        value = badge_data, overwrite=FALSE, append = TRUE)
    } else {
      # create table
      DBI::dbWriteTable(conn = pg_con, name = pgTable, 
                        value = badge_data)
    }
    }
  }

######### Figure for understanding wierd bayveiw data
# data_for_fb_df_jhh %>%
test2 %>%
  mutate(date = date(time_in)) %>%
  dplyr::select(date,badge,duration) %>%
  # filter(duration < 1440) %>%
  group_by(date, badge) %>%
  summarize(
    tot_duration = sum(duration, na.rm = TRUE),
    n_hits = n()) %>%
  ungroup() %>%
  ggplot(aes(x = n_hits, y = tot_duration)) + geom_point() + facet_wrap(~badge)

data_for_fb_df_jhh %>%
  mutate(date = date(time_in)) %>%
  filter(date == as.Date('2022-05-29')) %>%
  filter(badge == 524772) %>%
  summarize(tot_duration = sum(duration))

data_for_fb_df_jhh %>%
  mutate(date = date(time_in)) %>%
  group_by(date, badge) %>%
  summarize(tot_duration = sum(duration)) %>%
  filter(tot_duration >= 1440)




