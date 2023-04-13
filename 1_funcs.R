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
    datalist[[i]] <- read.csv(f_names[[i]]) %>% janitor::clean_names()
  }
  df <- do.call(rbind,datalist)

  rec_df <- df %>%
    select(receiver,receiver_name,receiver_recode) %>%
    distinct()
  if(!DBI::dbExistsTable(conn = con, name = 'rtls_receivers')) {
    DBI::dbCreateTable(con, 'rtls_receivers',rec_df)
  }
  DBI::dbWriteTable(con, name = 'rtls_receivers', value = rec_df, append = TRUE)

  df <- df %>%
    select(receiver,time_in,time_out,rtls_id) %>%
    mutate(
      time_in = lubridate::ymd_hms(time_in, tz = "America/New_York"),
      time_out = lubridate::ymd_hms(time_out, tz = "America/New_York")
    )
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
  # return(df)
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
    collect() #%>%
    # rename(receiver_recode = location_code)
  badge_data <- badge_data %>%
    left_join(receivers, by = 'receiver', multiple = "all")
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
        if (!plyr::empty(badge_data)) {
          badge_data$badge <- as.integer(stringr::str_split(badge, '_')[[1]][2])
          site_data <- rbind(site_data, badge_data)
        } else {print(paste('No data in range for',badge,'...'))}
      } else {print(paste('No Table for ',badge,' ...'))}
    } # End iterating through badges
    # location code all data
    print('Here 1')
    if (!plyr::empty(site_data)) {
      site_data <- locCodeBadges(
        badge_data = site_data,
        db_u = config$db_u,
        db_pw = config$db_pw,
        site = site) %>%
        mutate(site = site)
      print('Here 2')
      if (dim(all_data)[1] == 0) 
        { all_data <- data.frame(site_data)} # creates copy
      else { all_data <- dplyr::bind_rows(all_data,site_data)} # appends site data together
    }
    
  } # End site looping
  # print('Here 3')
  if (!plyr::empty(all_data)) {
    all_data$duration <- as.numeric(difftime(all_data$time_out,all_data$time_in,units = 'mins'))
  }
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

##############
##############  File processing
##############

get_task_lists <- function(data_dir, fname) {
  tl_df <- readxl::read_excel(here(data_dir,fname)) %>% janitor::clean_names()
  
  # set overall start_time
  tl_df <- tl_df %>%
    mutate(
      start_time = if_else(
        am_or_pm == 'am', update(date, hour = 7), update(date, hour = 19)
      )
    )
  # create segments within the shift
  # chunk_list <- list()
  chunked_df <- data.frame()
  for (shift_chunk in c(0,1,2)) {
    hrs <- shift_chunk*4
    chunk_df <- tl_df %>%
      mutate(
        shift_chunk = shift_chunk,
        start_time = start_time + hours(hrs)
      )
    chunked_df <- rbind(chunked_df,chunk_df)
    
  }
  
  chunked_df <- chunked_df %>%
    select(-c(rtls_in_db,rtls_final_export,soc_bdg_loaded, soc_bdg_final_export,e4_in_db,e4_exported)) %>%
    mutate(
      duration_min = 4*60, # 4 hour chunks
      start_time = force_tz(start_time, "America/New_York")#,
      # start_time = with_tz(start_time, "UTC")
    ) %>%
    group_by(shift_day, shift_chunk) %>% mutate(task_num = cur_group_id()) %>% ungroup()
  
  return(chunked_df)
}



#########################
#### Make and store metrics
#########################

get_rtls_metrics <- function(tasks_df) {
  
  data_to_update <- data.frame(
    'shift' = character(),
    'shift_chunk' = integer(),
    'rtls_id' = integer(),
    'entropy' = numeric(),
    'fano_factor' = numeric(),
    'min_in_pt_rm' = numeric(),
    'prop_time_in_pt_rm ' = numeric()
  )
  
  for (shift in unique(tasks_df$shift_day)) { # Loop through each shift
    print(shift)
    all_data <- data.frame()
    for (chunk in 0:2) { # iterate through each chunk
      print(chunk)
      # get badges and other info
      badges <- tasks_df[which(tasks_df$shift_day == shift & tasks_df$shift_chunk == chunk),] %>%
        select(rtls_id) %>%
        unlist() %>%
        unname()
      print(badges)
      strt <- unique(tasks_df[which(tasks_df$shift_day == shift & tasks_df$shift_chunk == chunk),'start_time'])$start_time

      duration_min <- unique(tasks_df[which(tasks_df$shift_day == shift & tasks_df$shift_chunk == chunk),'duration_min'])$duration_min
      stp <- strt + lubridate::minutes(duration_min)
      # pull data for each set of bages
      shift_df <- get_and_locCode_RTLS_data_pg(
        badges = badges,
        strt = strt, 
        stp = stp, 
        sites = 'picu', 
        use_rules = FALSE)
      
      for (b in unique(shift_df$badge)) {
        print(paste('Working on shift ',shift,'chunk',chunk,'and badge',b))

        # time in pt room
        min_in_pt_rm <- shift_df %>% 
          filter(badge == b) %>% 
          filter(receiver_recode == 'Patient room') %>% 
          select(duration) %>%
          sum()

        tot_time <- shift_df %>% 
          filter(badge == b) %>% 
          select(duration) %>%
          sum()
        prop_time_in_pt_rm <- min_in_pt_rm / tot_time

        b_data <- shift_df %>% filter(badge == b) %>%
          select(-c(badge,time_out,duration,receiver_name,receiver_recode,site)) %>%
          mutate(
            time_in = lubridate::floor_date(time_in, unit = "second")
          )

        b_ts <- data_frame('time_in' = seq(strt,by = 'sec', length.out = 4*60*60)) %>%
          left_join(b_data, by = 'time_in', multiple = "all") %>%
          fill(receiver, .direction = "down")
        entropy <- b_ts %>% 
          select(receiver) %>%
          drop_na() %>%
          table() %>%
          DescTools::Entropy()

        #burstiness
        fano_factor <- b_ts %>%
          mutate(
            receiver.lag = lag(receiver),
            transition = if_else(receiver == receiver.lag,0,1)
          ) %>%
          select(transition) %>%
          drop_na()
        fano_factor <- sd(fano_factor$transition) / mean(fano_factor$transition)

        newRow <- list(
          'shift' = shift,
          'shift_chunk' = as.integer(chunk),
          'rtls_id' = as.integer(b),
          'entropy' = entropy,
          'fano_factor' = fano_factor,
          'min_in_pt_rm' = min_in_pt_rm,
          'prop_time_in_pt_rm' = prop_time_in_pt_rm
        )
        data_to_update <- rbind(data_to_update, newRow)
        # print(data_to_update)
      }

    }
  }
  return(data_to_update)
}


## pull data for each set of badges
### get get_and_locCode_RTLS_data_pg



## structure the data frame
## create uniform timeline (use stard and end)
### Pad start and end of timeline with NAs
### forward fil receiver codes (from time in stamp)
## for each badge, create metrics BY segment (pay attention to am/pm)
## Store time in pt room by segment
### create entropy and burstiness measure

