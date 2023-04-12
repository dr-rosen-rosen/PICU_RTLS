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

csv_to_db <- function(top_folder,con) {
  # get list of all of the file names to read in
  f_names <- list.files(top_folder, 
                        pattern = "RTLS",
                        full.names = TRUE, 
                        recursive = TRUE
                        )
  datalist = vector("list", length = length(f_names))
  for (i in 1:length(f_names)) {
    # print(f_names[[i]])
    datalist[[i]] <- read.csv(f_names[[i]]) %>% janitor::clean_names()
    # print(nrow(datalist[[i]]))
  }
  df <- do.call(rbind,datalist)
  rec_df <- df %>%
    select(receiver,receiver_name,receiver_recode) %>%
    distinct()
  if(!DBI::dbExistsTable(conn = con, name = 'rtls_receivers')) {
    DBI::dbCreateTable(con, 'rtls_receivers',rec_df)
  }
  DBI::dbWriteTable(con, name = 'rtls_receivers', value = rec_df, append = TRUE)
  
  df %>%
    select(receiver,time_in,time_out,rtls_id)
  print(nrow(df))
  badges <- unique(df$rtls_id)
  print(badges)
  for (badge in badges) {
    # check if table exists for given badge.
    badge_df <- df %>%
      filter(rtls_id == badge) %>%
      select(receiver,time_in,time_out)
    print(badge)
    print(nrow(badge_df))
    t <- paste0('table_',badge)
    if (!DBI::dbExistsTable(conn = con, name = t)) {
      DBI::dbCreateTable(con,t,badge_df) # create new table if it does not exist
    }
    DBI::dbWriteTable(con, name = t, value = badge_df, append = TRUE) # write data
  }
}


top_folder <- '/Users/mrosen44/Johns Hopkins/Salar Khaleghzadegan - Project_CollectiveAllostaticLoad/PICU Data Collection'
csv_to_db(top_folder,
          con = get_connection(
            db_name = config$db_name, 
            db_u = config$db_u, 
            db_pw = config$db_pw
          ))

df <- read.csv("/Users/mrosen44/Johns Hopkins/Salar Khaleghzadegan - Project_CollectiveAllostaticLoad/PICU Data Collection/Shift_89/RTLS_data/Shift_89_RTLS.csv") %>%
  janitor::clean_names()

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

split_days <- function(row) {
  # new_time_out <- lubridate::date(row$time_in[[1]])
  # print(row)
  row <- as.data.frame(row)
  day_one <- list(
    'receiver' = row$receiver,#[[1]],
    "time_in" = row$time_in,
    "time_out" = NA,#row$time_out,#new_time_out,
    "badge" = row$badge,
    "receiver_recode" = row$receiver_recode,
    "receiver_name" = row$receiver_name,
    "duration" = NA #row$duration
  )
  day_two <- list(
    'receiver' = row$receiver,#[[1]],
    "time_in" = NA, #row$time_in,
    "time_out" = row$time_out,#new_time_out,
    "badge" = row$badge,
    "receiver_recode" = row$receiver_recode,
    "receiver_name" = row$receiver_name,
    "duration" = NA #row$duration
  )
  return(dplyr::bind_rows(list(day_one,day_two)))
}

locCodeBadges <- function(badge_data,db_u,db_pw,site) {
  # get receiver data
  con <- get_connection(
    db_name = paste0('rtls_',site),
    db_u = config$db_u,
    db_pw = config$db_pw)
  receivers <- con %>%
    tbl('rtls_receivers') %>%
    collect() %>%
    rename(receiver_recode = location_code)
  badge_data <- badge_data %>%
    left_join(receivers, by = 'receiver')
  return(badge_data)
}

get_and_locCode_RTLS_data_pg <- function(badges, strt, stp, sites, use_rules) {
  
  print("Pulling RTLS data...")
  all_data <- data.frame()
  for (site in sites){
    print(paste0("...for ",site,":"))
    # set up empty dataframe to store site results
    site_data <- data.frame(
      receiver = integer(),
      time_in = .POSIXct(character()),
      time_out = .POSIXct(character()),
      badge = character(),
      receiver_recode = character(),
      receiver_name = character(),
      duration = numeric()
    )

    con <- get_connection(
      db_name = paste0('rtls_',site),
      db_u = config$db_u,
      db_pw = config$db_pw)

    # makes sure badges is a list (and not just one badge var)
    if (length(badges) > 1){ site_badges <- paste0('table_',badges)}
    else if (length(badges) == 1) {
      if (badges == 'all') {
        tbls <- DBI::dbListTables(con)
        site_badges <- grep('table_',tbls, value = TRUE)
    } else {site_badges <- as.list(paste0('table_',badges))}
    }

    # loops through each badge and returns data in timerange
    for (badge in site_badges) {
      print(badge)
      if (DBI::dbExistsTable(conn = con, name = badge)) {
        # Read in data and filter by time
        badge_data <- con %>%
          tbl(badge) %>%
          collect() %>%
          mutate(across(c('time_in','time_out'), lubridate::ymd_hms)) %>%
          filter(time_in > strt & time_in < stp) # start and stop times are non-inclusive
        if (!plyr::empty(df)) {
          badge_data$badge <- as.integer(stringr::str_split(badge, '_')[[1]][2])
          site_data <- rbind(site_data, badge_data)
        } else {print(paste('No data in range for',badge,'...'))}
      } else {print(paste('No Table for ',badge,' ...'))}
    } # End iterating through badges
    # location code all data
    # print('Here 1')
    site_data <- locCodeBadges(
      badge_data = site_data,
      db_u = config$db_u,
      db_pw = config$db_pw,
      site = site) %>%
      mutate(site = site)
    # print('Here 2')
    if (dim(all_data)[1] == 0) 
      { all_data <- data.frame(site_data)} # creates copy
    else { all_data <- dplyr::bind_rows(all_data,site_data)} # appends site data together
    
  } # End site looping
  # print('Here 3')
  all_data$duration <- as.numeric(difftime(all_data$time_out,all_data$time_in,units = 'mins'))
  # applies locatio screening rules.
  #
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
