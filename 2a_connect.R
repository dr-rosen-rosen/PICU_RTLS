###############################################################################################
###############################################################################################
#####################   Connection for accessing db directly from R (most runs through python)
#####################
###############################################################################################
###############################################################################################

library(tidyverse)
library(RSQLite)
library(config)
config <- config::get()
# in case db loc is not in project directory
prgdir <- getwd()
setwd(config$db_loc)
# connect to RTSL database
con <- DBI::dbConnect(RSQLite::SQLite(), dbname = config$db_name)
setwd(prgdir)
DBI::dbListTables(con)

