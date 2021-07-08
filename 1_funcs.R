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
###############################################################################################
#####################   Pulls data for specific badges and time range
###############################################################################################

get_RTLS_data <- function(badges, strt, stp) {
  # set up empty dataframe to store
  print("Pulling RTLS data...")
  all_data <- data.frame(
    Receiver = integer(),
    Time_In = .POSIXct(character()),
    Time_Out = .POSIXct(character()),
    Badge = character()
  )
  # makes sure badges is a list (and not just one badge var)
  if (length(badges) > 1){ badges <- paste0('Table_',badges)}
  else if (length(badges) == 1) {
    if (badges == 'all') {
      tbls <- DBI::dbListTables(con)
      badges <- grep('Table_',tbls, value = TRUE)
    } else {badges <- as.list(paste0('Table_',badges))}
    }

  # loops through each badge and returns data in timerange
  for (badge in badges) {
    if (DBI::dbExistsTable(conn = con, name = badge)) {
      badge_data <- con %>%
      tbl(badge) %>%
      collect() %>%
      mutate(across(c('Time_In','Time_Out'), lubridate::ymd_hms))
    # Set time filters
      badge_data <- badge_data %>% filter(Time_In >= as.Date(strt) & Time_Out <= as.Date(stp))
    #else if (strt != 'all' & stp == 'all') {badge_data <- badge_data %>% filter(Time_In >= strt)} 
    #else if (strt == 'all' & stp != 'all') {badge_data <- badge_data %>% filter(Time_Out <= stp)} 
    
      badge_data$Badge <- as.integer(stringr::str_split(badge, '_')[[1]][2])
      all_data <- rbind(all_data, badge_data)
      print(paste(nrow(badge_data),'rows for badge',badge))
    } else {print(paste('No Table for ',badge,' ...'))}
  }
  return(all_data)
}

###############################################################################################
#####################   Creates a bar charts
###############################################################################################

make_overall_bar <- function(df, badge){

  df$Receiver_recode <- factor(df$Receiver_recode, 
                        levels = c('Patient room','MD Workroom','Ward Hall','Education','Supply and admin','Transit','OTHER/UNKNOWN','Family waiting space'))
  df <- df %>% transform(
    Receiver_recode=plyr::revalue(Receiver_recode,c("OTHER/UNKNOWN"="Other",'Family waiting space'='Family space')))
  ### structure data
  overall_summary <- df %>% group_by(Receiver_recode) %>%
    summarise(total_time = sum(Duration,na.rm = TRUE)) %>%
    mutate(proportion_time = (total_time / sum(total_time, na.rm = TRUE)) * 100 )
  overall_summary$Source <- 'All badges'
  badge_summary <- df %>% filter(Badge == badge) %>%
    group_by(Receiver_recode) %>%
    summarize(total_time = sum(Duration, na.rm = TRUE)) %>%
    mutate(proportion_time = (total_time / sum(total_time, na.rm = TRUE)) * 100 )
  badge_summary$Source <- paste('Badge',badge)
  summary <- rbind(overall_summary,badge_summary)
  
  ## overall figure
  summary_fig <- summary %>% ggplot() +
    aes(x = Receiver_recode, fill = Source, weight = proportion_time) +
    geom_bar(position = "dodge") +#, fill = "#0c4c8a") +
    scale_fill_viridis(discrete = TRUE, option = 'C') +
    theme_classic() +
    theme(axis.text.x = element_text(angle = 90, hjust=0.95), legend.position=c(.85,1), legend.title = element_blank()) +
    labs(
      title = 'Proportion of time spent in locations',
      subtitle = paste('From',lubridate::date(min(df$Time_In)),'to',lubridate::date(max(df$Time_Out))),
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
  
  df$Location <- factor(df$Location, 
                        levels = c('Patient room','MD Workroom','Ward Hall','Education','Supply and admin','Transit','OTHER/UNKNOWN','Family waiting space'))
  df <- df %>% transform(
            Location=plyr::revalue(Location,c("OTHER/UNKNOWN"="Other",'Family waiting space'='Family space')))
  #inserts 0 for all missing location by hour combintions to avoid gaps on area chart
  combinations <- expand.grid(hour = unique(df$hour), Location = unique(df$Location))
  df <- df %>%
    full_join(combinations, by = c('hour' = 'hour','Location' = 'Location')) %>%
    mutate(Duration = ifelse(is.na(Duration), 0, Duration)) %>%
    arrange(hour, Location)
  if (perc) {
    plt <- df %>%
      group_by(hour, Location) %>%
      summarise(n = sum(Duration)) %>%
      mutate(percentage = n / sum(n) * 100) %>%
      ggplot(
        aes(x=hour, y = percentage, fill = Location)
      )
    plt <- plt + scale_y_continuous(labels = scales::percent_format(scale = 1))
    metric_title_string <- "Percent"
    y_lab_text <- "Percentage of time"
  } else {
  plt <- df %>% ggplot(
         aes(x=hour, y=Duration,fill=Location))
  metric_title_string <- "Total"
  y_lab_text <- "Minutes"
  }
  plt <- plt +
    geom_area(colour="white") + #alpha=0.6 , size=.5,
    scale_fill_viridis(discrete = T) +
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

create_FB_reports <- function(target_badges, strt, stp, FB_report_dir) {
  # pull and process data
  df <- get_RTLS_data(
    badges = target_badges,
    strt = strt,
    stp = stp
  )

  df <- loc_code_badge_data(
    badge_data = df,
    db_name = config$db_name,
    db_loc = config$db_loc
  )

  df <- apply_rules(
    df = df,
    rule_1_thresh = config$rule_1_thresh,
    rule_2_thresh = config$rule_2_thresh,
    rule_2_locs = config$rule_2_locs
  )

  df <- df %>%
    group_by(Badge) %>%
    filter(sum(Duration) > (config$min_hours_for_fb * 60))

  # make reports
  for (badge in unique(df$Badge)){
    overall_bar <- make_overall_bar(df = df, badge = badge)
    ind_area_norm <- make_area_plot(
      df = make_timeseries_df_for_dummies(df = df[which(df$Badge == badge), ]),#,f = 'S'),
      perc = TRUE, #if true, will create porportional chart ,if false will do raw
      badge = badge # if NULL this assumes a summary plot; if an int it will use that in plot titles
    )
    ind_area_raw <- make_area_plot(
      df = make_timeseries_df_for_dummies(df = df[which(df$Badge == badge), ]),#,f = 'S'),
      perc = FALSE, #if true, will create porportional chart ,if false will do raw
      badge = badge # if NULL this assumes a summary plot; if an int it will use that in plot titles
    )
    tot_hours <-sum(df[which(df$Badge == badge),'Duration']) / 60
    tot_hours <- round(tot_hours,digits = 1)
    area_plots <- ind_area_raw / ind_area_norm + plot_layout(guides = 'collect') & theme(legend.position='bottom')
    report <- officer::read_docx(path = config$FB_report_template) %>%
      body_add_par("Time in Location Data Report", style = "Title") %>%
      body_add_par(paste("Badge:",toString(badge)), style = 'Normal') %>%
      body_add_par(paste("From:",lubridate::date(min(df$Time_In)),"to",lubridate::date(max(df$Time_Out))), style = 'Normal') %>%
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
  }
}
