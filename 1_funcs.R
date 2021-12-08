###############################################################################################
###############################################################################################
#####################   FUNCS for Running Feedback Reports for RTLS data
#####################
###############################################################################################
###############################################################################################
library(lubridate)
library(tidyverse)
library(viridis)
library(hrbrthemes)
library(officer)
library(DBI)
library(network)
library(here)
library(Microsoft365R)

####################################################################################################
############################## DB connection (pg) and management
####################################################################################################

get_connection <- function(db_name, db_u, db_pw){
    con <- DBI::dbConnect(RPostgres::Postgres(),
                           dbname   = db_name,
                           host     = 'localhost',
                           port     = 5433,
                           user     = db_u,
                           password = db_pw)
    return(con)
}

get_sqlite_con <- function(db_loc, db_name){
    prgdir <- getwd()
    setwd(db_loc)
    # connect to RTSL database
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db_name)
    setwd(prgdir)
    return(con)
}

check_receiver_overlap <- function(){
    jhh <- get_receiver_loc_data(
        con = get_connection(
        db_name = paste0('rtls_','jhh'),
        db_u = config$db_u,
        db_pw = config$db_pw),
        t_name = 'rtls_receivers')
    bmc <- get_receiver_loc_data(
        con = get_connection(
        db_name = paste0('rtls_','bmc'),
        db_u = config$db_u,
        db_pw = config$db_pw),
        t_name = 'rtls_receivers')
    old_jhh <- get_receiver_loc_data(
        con = get_sqlite_con(
        db_loc = config$db_loc,
        db_name = config$db_name),
        t_name = 'rtls_receivers')
    pg_jhh_bmc <- intersect(
        unique(jhh$receiver),
        unique(bmc$receiver)
    )
    old_new_jhh <- setdiff(
        unique(old_jhh$Receiver),
        unique(jhh$receiver)
    )
    return(list(pg_jhh_bmc_overlap = pg_jhh_bmc, old_not_in_new_jhh = old_new_jhh))
}

migrate_location_codes <- function(pg_con, sqlite_con){
  old_loc_codes <- get_receiver_loc_data(
      con = sqlite_con,
      t_name = 'RTLS_Receivers'
  )
  manual_receiver_update(
      df = old_loc_codes,
      con = pg_con
  )
  #DBI::dbDisconnect(pg_con)
  #DBI::dbDisconnect(sqlite_con)
  NULL
}
###############################################################################################
#####################   Pulls data for specific badges and time range
###############################################################################################

get_RTLS_data <- function(badges, strt, stp, con) {
  # set up empty dataframe to store
  print("Pulling RTLS data...")
  all_data <- data.frame(
    receiver = integer(),
    time_in = .POSIXct(character()),
    time_out = .POSIXct(character()),
    badge = character()
  )
  # makes sure badges is a list (and not just one badge var)
  if (length(badges) > 1){ badges <- paste0('table_',badges)}
  else if (length(badges) == 1) {
    if (badges == 'all') {
      tbls <- DBI::dbListTables(con)
      badges <- grep('table_',tbls, value = TRUE)
  } else {badges <- as.list(paste0('table_',badges))}
    }
  # loops through each badge and returns data in timerange
  for (badge in badges) {
    if (DBI::dbExistsTable(conn = con, name = badge)) {
      # Read in data and filter by time
      badge_data <- con %>%
      tbl(badge) %>%
      collect() %>%
      mutate(across(c('time_in','time_out'), lubridate::ymd_hms)) %>%
      filter(time_in > strt & time_in < stp) # start and stop times are non-inclusive

      badge_data$badge <- as.integer(stringr::str_split(badge, '_')[[1]][2])
      all_data <- rbind(all_data, badge_data)
    } else {print(paste('No Table for ',badge,' ...'))}
  }
  return(all_data)
}


get_and_locCode_RTLS_data_pg <- function(badges, strt, stp, sites, use_rules) {
  # set up empty dataframe to store
  print("Pulling RTLS data...")
  all_data <- data.frame(
    receiver = integer(),
    time_in = .POSIXct(character()),
    time_out = .POSIXct(character()),
    badge = character(),
    receiver_recode = character(),
    receiver_name = character(),
    duration = numeric()
  )
  for (site in sites){
    con <- get_connection(
      db_name = paste0('rtls_',site),
      db_u = config$db_u,
      db_pw = config$db_pw)
    # makes sure badges is a list (and not just one badge var)
    if (length(badges) > 1){ badges <- paste0('table_',badges)}
    else if (length(badges) == 1) {
      if (badges == 'all') {
        tbls <- DBI::dbListTables(con)
        badges <- grep('table_',tbls, value = TRUE)
    } else {badges <- as.list(paste0('table_',badges))}
    }
    # loops through each badge and returns data in timerange
    for (badge in badges) {
      if (DBI::dbExistsTable(conn = con, name = badge)) {
        # Read in data and filter by time
        badge_data <- con %>%
          tbl(badge) %>%
          collect() %>%
          mutate(across(c('time_in','time_out'), lubridate::ymd_hms)) %>%
          filter(time_in > strt & time_in < stp) # start and stop times are non-inclusive

        badge_data$badge <- as.integer(stringr::str_split(badge, '_')[[1]][2])
        ### Add location coding
        # badge_data <- loc_code_badge_data_pg(
        #   badge_data = badge_data,
        #   db_u = config$db_u,
        #   db_pw = config$db_pw,
        #   site = site)
        all_data <- rbind(all_data, badge_data)
      } else {print(paste('No Table for ',badge,' ...'))}
    } # End iterating through badges
    all_data <- loc_code_badge_data_pg(
      badge_data = all_data,
      db_u = config$db_u,
      db_pw = config$db_pw,
      site = site)
  } # End site looping
  # if (use_rules == TRUE) {
  #   all_data <- apply_rules(
  #     df = all_data,
  #     rule_1_thresh = config$rule_1_thresh,
  #     rule_2_thresh = config$rule_2_thresh,
  #     rule_2_locs = config$rule_2_locs
  #   )
  # } # applies locatio screening rules.
  return(all_data)
}


# pulls all reciever location data... used for manual review and update
get_receiver_loc_data <- function(con, t_name) {
  receiver_data <- con %>%
    tbl(t_name) %>%
    collect()
  return(receiver_data)
}

manual_receiver_update <- function(df, con) {
  for (i in rownames(df)) {
    update_stmt <- paste0("UPDATE rtls_receivers ",
                       "SET location_code = ",paste0('\'',df[i,'location_code'],'\''),
                       " WHERE receiver = ",df[i,"receiver"],";")
    print(update_stmt)
    res <- DBI::dbExecute(con, update_stmt)
    print(res)
    #DBI::dbClearResult(res)
    #DBI::dbSendQuery(con, update_stmt)
  }
  NULL
}
###############################################################################################
#####################   Creates a bar charts
###############################################################################################

make_overall_bar <- function(df, badge_num){

  df$receiver_recode <- factor(df$receiver_recode,
                        levels = c('Patient room','MD Workroom','Ward Hall','Education','Supply and admin','Transit','OTHER/UNKNOWN','Family waiting space'))
  df <- df %>% transform(
    receiver_recode=plyr::revalue(receiver_recode,c("OTHER/UNKNOWN"="Other",'Family waiting space'='Family space')))
  ### structure data
  overall_summary <- df %>% group_by(receiver_recode) %>%
    summarise(total_time = sum(duration,na.rm = TRUE)) %>%
    mutate(proportion_time = (total_time / sum(total_time, na.rm = TRUE)) * 100 )
  overall_summary$Source <- 'All badges'
  badge_summary <- df %>% filter(badge == badge_num) %>%
    group_by(receiver_recode) %>%
    summarize(total_time = sum(duration, na.rm = TRUE)) %>%
    mutate(proportion_time = (total_time / sum(total_time, na.rm = TRUE)) * 100 )
  badge_summary$Source <- paste('Badge',badge_num)
  summary <- rbind(overall_summary,badge_summary) %>% drop_na(receiver_recode)

  ## overall figure
  summary_fig <- summary %>% ggplot() +
    aes(x = receiver_recode, fill = Source, weight = proportion_time) +
    geom_bar(position = "dodge") +#, fill = "#0c4c8a") +
    scale_fill_viridis(discrete = TRUE, option = 'C') +
    theme_classic() +
    theme(axis.text.x = element_text(angle = 90, hjust=0.95), legend.position=c(.85,1), legend.title = element_blank()) +
    labs(
      title = 'Proportion of time spent in locations',
      subtitle = paste('From',lubridate::date(min(df$time_in)),'to',lubridate::date(max(df$time_out))),
      x = 'Locations',
      y = 'Proportion of time'
    ) + scale_y_continuous(labels = scales::percent_format(scale = 1))

  return(summary_fig)
}

###############################################################################################
#####################   Creates area plots
###############################################################################################

make_area_plot <- function(df, perc, badge) {

  if (is.null(badge)) {
    source_title_str <- 'all badges'
  } else {
    source_title_str <- paste('badge',badge)
  }

  df$location <- factor(df$location,
                        levels = c('Patient room','MD Workroom','Ward Hall','Education','Supply and admin','Transit','OTHER/UNKNOWN','Family waiting space'))
  df <- df %>% transform(
            location=plyr::revalue(location,c("OTHER/UNKNOWN"="Other",'Family waiting space'='Family space')))
  #inserts 0 for all missing location by hour combintions to avoid gaps on area chart
  combinations <- expand.grid(hour = unique(df$hour), location = unique(df$location))
  df <- df %>%
    full_join(combinations, by = c('hour' = 'hour','location' = 'location')) %>%
    mutate(duration = ifelse(is.na(duration), 0, duration)) %>%
    arrange(hour, location)
  if (perc) {
    plt <- df %>%
      group_by(hour, location) %>%
      summarise(n = sum(duration)) %>%
      mutate(percentage = n / sum(n) * 100) %>%
      ggplot(
        aes(x=hour, y = percentage, fill = location)
      )
    plt <- plt + scale_y_continuous(labels = scales::percent_format(scale = 1))
    metric_title_string <- "Percent"
    y_lab_text <- "Percentage of time"
  } else {
  plt <- df %>% ggplot(
         aes(x=hour, y=duration,fill=location))
  metric_title_string <- "Total"
  y_lab_text <- "Minutes"
  }
  plt <- plt +
    geom_area(colour="white") + #alpha=0.6 , size=.5,
    scale_fill_viridis(discrete = TRUE, direction = -1) +
    #theme_ipsum() +
    theme_light() +
    labs(
      title = paste(metric_title_string,"Time Spent in Locations"),
      # subtitle = paste('For',source_title_str,'from',lubridate::date(min(df$Time_In)),'to',lubridate::date(max(df$Time_Out))),
      y = y_lab_text,
      x = 'Hour of the day'
    ) +
    scale_x_continuous(breaks = seq(0, 23, by = 4))
  return(plt)
}

###############################################################################################
#####################   Functions for creating feedback reports
###############################################################################################

### Creates one feedback report

create_FB_reports <- function(df, FB_report_dir, save_badge_timeline) {

  df <- df %>%
    group_by(badge) %>%
    filter(sum(duration) > (config$min_hours_for_fb * 60)) %>%
    ungroup()

  # make reports
  for (badge in unique(df$badge)){
    overall_bar <- make_overall_bar(df = df, badge_num = badge)
    ind_area_norm <- make_area_plot(
      df = make_timeseries_df_for_dummies(df = df[which(df$badge == badge), ]),#,f = 'S'),
      perc = TRUE, #if true, will create porportional chart ,if false will do raw
      badge = badge # if NULL this assumes a summary plot; if an int it will use that in plot titles
    )
    ind_area_raw <- make_area_plot(
      df = make_timeseries_df_for_dummies(df = df[which(df$badge == badge), ]),#,f = 'S'),
      perc = FALSE, #if true, will create porportional chart ,if false will do raw
      badge = badge # if NULL this assumes a summary plot; if an int it will use that in plot titles
    )
    tot_hours <-sum(df[which(df$badge == badge),'duration']) / 60
    tot_hours <- round(tot_hours,digits = 1)
    area_plots <- ind_area_raw / ind_area_norm + plot_layout(guides = 'collect') & theme(legend.position='bottom')
    report <- officer::read_docx(path = config$FB_report_template) %>%
      body_add_par("Time in Location Data Report", style = "Title") %>%
      body_add_par(paste("Badge:",toString(badge)), style = 'Normal') %>%
      body_add_par(paste("From:",lubridate::date(min(df$time_in)),"to",lubridate::date(max(df$time_out))), style = 'Normal') %>%
      body_add_par(paste("Total hours in this report:",toString(tot_hours)), style = 'Normal') %>%
      body_add_par('') %>%
      body_add_par(config$FB_intro_para) %>%
      body_add_par('') %>%
      body_add_par("Where does the data come from?", style = 'Subtitle') %>%
      body_add_par(config$FB_where_para, style = 'Normal') %>%
      body_add_par('') %>%
      body_add_par('What am I supposed to do with this data?', style = 'Subtitle') %>%
      body_add_par(config$FB_what_para, style = 'Normal') %>%
      body_add_break(pos = "after") %>%
      body_add_par("Where you spend your time compared to your peers", style = 'Subtitle') %>%
      body_add_par('') %>%
      body_add_gg(overall_bar) %>%
      body_add_break(pos = "after") %>%
      body_add_par("Where you spend your time throughout the day", style = 'Subtitle') %>%
      body_add_par('') %>%
      body_add_gg(area_plots) %>%
      print(target = file.path(getwd(),FB_report_dir,paste0(toString(badge),'.docx')))
    if (save_badge_timeline == TRUE) {
      write.csv(df[which(df$badge == badge), ],file.path(getwd(),FB_report_dir,paste0(toString(badge),'_timeline.csv')))
    }
  }
}

###############################################################################################
#####################   Functions for network metrics and visualization
###############################################################################################

prep_net_data <- function(df) {

  # This funciton takes an RTLS df and creates files for:
  #   Edges
  #   Nodes
  nodes <- df %>%
    group_by(receiver) %>%
    summarize(duration = sum(duration)) %>%
    ungroup() %>%
    mutate(id = row_number() - 1) %>%
    left_join(
      distinct(df,receiver,receiver_recode, receiver_name),
      by = "receiver"
    ) %>%
    rename(rec_num = receiver,
           type = receiver_recode,
           description = receiver_name) %>%
    arrange(desc(duration))

  #adding back
  distinct(df,receiver,receiver_recode)

  df <- relabel_nodes(df,nodes) # this just recodes the reciever id to the 'id' var from above; why is that so hard in R?

  edges <- df %>%
    arrange(time_in) %>% # makes sure rows are ordered in ascending time order
    mutate(to = lead(receiver)) %>% # creates new colum for destination link based on shifted Receiver column
    na.omit() %>% # drops last row of NA created by shifting
    rename(from = receiver)  %>% # renames Receiver column to the 'from' end of edge
    group_by(from, to) %>%
    summarize(weight = n()) %>%
    ungroup()

  return(list('nodes' = nodes, 'edges' = edges))
}

###############################################################################################
#####################   Functions for automating data upload from email
###############################################################################################

get_files_from_outlk <- function(outlk_sess, n) {
  # Go to the folder for RTLS data
  folder <- outlk_sess$get_folder('RTLS_Data')
  tst_list <- folder$list_emails(n=n)

  for (em in tst_list) {
    # Check source of data
    if (grepl("BMC_RTLS-Reports",em$properties$sender$emailAddress$name, fixed = TRUE)) {
      print('Bayview')
      site <- "bmc"
    } else if (grepl("RTLSDB-Alerts", em$properties$sender$emailAddress$name, fixed = TRUE)) {
      print('jhh')
      site <- "jhh"
    } else if (grepl("Versus Reports", em$properties$sender$emailAddress$name, fixed = TRUE)) {
      site <- "battery"
      print(site)
    } else {
      site <- NA
      print('huh?')
    }

    #test file type
    if (grepl('.csv',em$list_attachments()[[1]]$properties$name,fixed = TRUE)){
      print('csv')
      kind <- '.csv'
      #em$list_attachments()[[1]]$download(dest = here('test.csv'),overwrite = TRUE)
    } else if (grepl('.pdf',em$list_attachments()[[1]]$properties$name,fixed = TRUE)){
      print('pdf')
      kind <- '.pdf'
    } else {
      kind <- NA
    }

    if (!is.na(site) & !is.na(kind)) {
      # Download attachemnt
      f <- paste0('rtls_',site,'_',lubridate::date(em$properties$sentDateTime),kind)
      em$list_attachments()[[1]]$download(dest = here('Data/RTLS_Data/tmp',f),overwrite = TRUE)
      # move email to archive folder
      em$move(dest = outlk_sess$get_folder('RTLS_archive'))
    }
  }
  NULL
}
