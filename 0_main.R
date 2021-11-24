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
reticulate::source_python('1_funcs_pg.py')
source(here('1_funcs.R'), echo = TRUE)
#source(here('2a_connect.R'), echo = TRUE)

########## Loads in automated csv files
########## OLD SQLITE VERSION
# csv_to_db(
#   db_name = config$db_name,
#   db_loc = config$db_loc,
#   in_path = config$in_path
#   )

######### Automated data flow from email to postgres

get_files_from_outlk(
  outlk_sess = Microsoft365R::get_business_outlook()
)

csv_to_db_pg(
  tmp_csv_path = config$tmp_csv_path, 
  archive_csv_path = config$archive_csv_path,
  db_u = config$db_u, 
  db_pw = config$db_pw)

get_weekly_report_pg(
  anchor_date = lubridate::today(),
  look_back_days = 9,
  db_u = config$db_u,
  db_pw = config$db_pw,
  target_badges = get_active_badges(config$badge_file),
  weekly_report_dir = config$weekly_report_dir)

data_for_fb_df <- get_and_locCode_RTLS_data_pg(
  badges = get_active_badges(config$badge_file), 
  strt = lubridate::ymd('2021-11-7'), 
  stp = lubridate::ymd('2021-11-22'), 
  sites = c('jhh'),
  use_rules = TRUE
)
nrow(data_for_fb_df)

create_FB_reports(
  df = data_for_fb_df,
  FB_report_dir = config$FB_report_dir,
  save_badge_timeline = FALSE
)

make_overall_bar(df = data_for_fb_df, badge = 524787)


########## Updates receiver location codes
rcvr_dscrp_to_loc_code(
  db_name = config$db_name,
  db_loc = config$db_loc,
  rcvr_recode_file = config$rcvr_recode_file,
  rcvr_recode_file_loc = config$rcvr_recode_file_loc,
  print_new_recievers = TRUE
  )

########## Manul review and update of locations in table
# returns overlap between receiver id's at jhh and bmc
x <- check_receiver_overlap()
#takes location codes from old DB and pushes to pg
migrate_location_codes(
  pg_con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  sqlite_con = get_sqlite_con(
    db_loc = config$db_loc,
    db_name = config$db_name
  )
)
bmc <- get_receiver_loc_data(
  con = get_connection(
    db_name = paste0('rtls_','bmc'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  t_name = 'rtls_receivers')

old_loc_codes <- get_receiver_loc_data(
  con = get_sqlite_con(
    db_loc = config$db_loc,
    db_name = config$db_name
  ),
  t_name = 'RTLS_Receivers'
)
sqlite_con <- get_sqlite_con(
  db_loc = config$db_loc,
  db_name = config$db_name
)

get_receiver_loc_data(
  con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  t_name = 'RTLS_Receivers') #%>%
  #select(Receiver) %>% unique(.)

write_csv(bmc, file = 'bmc_receiver_recode.csv')
#df <- read.csv('receiver_recode_reviewed.csv')
manual_receiver_update(df = df, con = con)


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
  target_badges = get_active_badges(config$badge_file),
  weekly_report_dir = config$weekly_report_dir
  )

test <- get_active_badges(config$badge_file)
audit_active_badges(
  con = con,
  active_badges = get_active_badges(config$badge_file)
)
# create_FB_reports(
#   target_badges = get_active_badges(config$badge_file),
#   strt = config$FB_report_start,
#   stp = config$FB_report_stop,
#   FB_report_dir = config$FB_report_dir,
#   save_badge_timeline = FALSE
# )

# load some badge data
x <- get_RTLS_data(
  badges = '526896',  #get_active_badges(config$badge_file),#'all',#badges,
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
