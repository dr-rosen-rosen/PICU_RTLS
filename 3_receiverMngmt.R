###############################################################################################
###############################################################################################
#####################   FUNCS for Managing RTLS receivers data
#####################
###############################################################################################
###############################################################################################


library(tidyverse)

all_data <- getReceiverHitData(
  con = get_connection(
    db_name = paste0('rtls_','bmc'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  receivers = paulsReceivers#c(650)
)
all_data <- loc_code_badge_data_pg(
  badge_data = all_data,
  db_u = config$db_u,
  db_pw = config$db_pw,
  site = 'bmc')

recSumData <- all_data %>%
  group_by(receiver_name) %>%
  summarise(n = n(), 
            tot_duration = sum(duration), 
            avg = mean(duration), 
            sd = sd(duration))


hist(all_data[which(all_data$receiver_name == 'Default Location'),'duration'],breaks = 100)

# Pulls data for receivers from across badge tables

getReceiverHitData <- function(con,receivers){
  
  # Get all badgee tables
  tbls <- DBI::dbListTables(con)
  tbls <- stringr::str_subset(tbls,pattern = 'table_')
  
  # dataframe to hold results
  all_data <- data.frame(
    receiver = integer(),
    time_in = .POSIXct(character()),
    time_out = .POSIXct(character()),
    badge = character()
  )
  for (receiver in receivers) {
    for (tbl in tbls) {
      badge_data <- con %>%
        tbl(tbl) %>%
        filter(receiver == !!as.integer(receiver)) %>%
        #show_query()
        collect() %>%
        mutate(badge = as.integer(stringr::str_split(tbl, '_')[[1]][2]))
      all_data <- rbind(all_data, badge_data)
    }
  }
  
  return(all_data)
}


bmc<- get_receiver_loc_data(
  con = get_connection(
    db_name = paste0('rtls_','bmc'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  t_name = 'rtls_receivers')
write_csv(bmc, file = 'bmc_receiver_recode3.csv')
#df <- read.csv('receiver_recode_reviewed.csv')
manual_receiver_update(
  df = read.csv('bmc_receiver_recode3.csv'), 
  con = get_connection(
    db_name = paste0('rtls_','bmc'),
    db_u = config$db_u,
    db_pw = config$db_pw)
)

jhh <- get_receiver_loc_data(
  con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  t_name = 'rtls_receivers')
write_csv(jhh, file = 'jhh_receiver_recode3.csv')
#df <- read.csv('receiver_recode_reviewed.csv')
manual_receiver_update(
  df = read.csv('jhh_receiver_recode3.csv'), 
  con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw)
)



############### OLD
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

jhh <- get_receiver_loc_data(
  con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw),
  t_name = 'rtls_receivers') #%>%
#select(Receiver) %>% unique(.)

write_csv(bmc, file = 'bmc_receiver_recode.csv')
write_csv(jhh, file = 'jhh_receiver_recode.csv')
#df <- read.csv('receiver_recode_reviewed.csv')

manual_receiver_update(
  df = read.csv('jhh_receiver_recode3.csv'), 
  con = get_connection(
    db_name = paste0('rtls_','jhh'),
    db_u = config$db_u,
    db_pw = config$db_pw)
)
