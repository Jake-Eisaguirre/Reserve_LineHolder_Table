print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc, padr)


current_date <- Sys.Date()  # Today's date
week_prior <- current_date - 3  # Date three days prior
week_prior_pairing_date <- current_date - 7  # Date seven days prior
previous_bid_period <- substr(as.character((current_date)), 1, 7)

raw_date <- Sys.Date()

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),
                                  Driver = "SnowflakeDSIIDriver",
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                  WAREHOUSE = "DATA_LAKE_READER",
                                  Database = "ENTERPRISE",
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                  authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")

# Query master schedule data and clean it
q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")
view_masterhistory <- dbGetQuery(db_connection, q_master_history)



clean_mh <- view_masterhistory %>% 
  select(CREW_INDICATOR, PAIRING_POSITION, CREW_ID, PAIRING_DATE, BASE, BID_PERIOD, LINE_TYPE, UPDATE_DATE, UPDATE_TIME) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep=" ")) %>% 
  group_by(CREW_INDICATOR, PAIRING_POSITION, CREW_ID, PAIRING_DATE) %>%
  filter(update_dt == max(update_dt),
         LINE_TYPE == "B") %>% 
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", 
                         "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", 
                         "32", "33", "34", "36", "37")) %>% 
  filter(!BASE == "CVG") %>% 
  mutate(PAIRING_POSITION = if_else(PAIRING_POSITION == "RO", "FO", PAIRING_POSITION),
         PAIRING_POSITION = if_else(CREW_INDICATOR == "FA", "FA", PAIRING_POSITION)) %>% 
  drop_na() %>% 
  ungroup()


q_master_sched <- paste0("select CREW_ID, EQUIPMENT, PAIRING_POSITION, BID_DATE as BID_PERIOD
                   from CT_MASTER_SCHEDULE WHERE BID_DATE ='",previous_bid_period, "';")


raw_ms <- dbGetQuery(db_connection, q_master_sched)


clean_ms <- raw_ms%>% 
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", 
                         "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", 
                         "32", "33", "34", "36", "37")) %>% 
  group_by(PAIRING_POSITION, CREW_ID, BID_PERIOD) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  filter(!EQUIPMENT == "33Y") %>% 
  mutate(EQUIPMENT = if_else(PAIRING_POSITION == "FA", NA, EQUIPMENT))


### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")


res_utl_q <- paste0("select * from AA_RESERVE_UTILIZATION WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")

raw_reserve_utl <- dbGetQuery(db_connection_pg, res_utl_q)

clean_reserve_utl <- raw_reserve_utl %>% 
  select(PAIRING_POSITION, PAIRING_DATE, BASE, RLV_SCR, EQUIPMENT) %>% 
  rename(R = RLV_SCR) %>% 
  mutate(BASE = if_else(PAIRING_POSITION %in% c("CA", "FO") & EQUIPMENT == "717", "HAL", BASE),
         BASE = if_else(PAIRING_POSITION %in% c("CA", "FO") & !EQUIPMENT == "717", "HNL", BASE)) %>% 
  select(!EQUIPMENT)



gold_ms <- clean_mh %>% 
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION")) %>% 
  group_by(PAIRING_POSITION, PAIRING_DATE, BASE, EQUIPMENT, LINE_TYPE) %>% 
  reframe(DAILY_COUNT = n()) %>% 
  pivot_wider(names_from = LINE_TYPE, values_from = DAILY_COUNT) %>% 
  inner_join(clean_reserve_utl, by = c("PAIRING_POSITION", "PAIRING_DATE", "BASE")) %>% 
  mutate(reserve_lineholder_perc = round((R/B)*100, 2))


present_ut <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_RESERVE_LINEHOLDER")


# Find matching columns between the present and final pairings
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(gold_ms))

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- gold_ms %>%
  select(matching_cols)


final_append <- anti_join(final_append_match_cols, match_present_fo, by = join_by(PAIRING_POSITION, PAIRING_DATE, EQUIPMENT, BASE))

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_RESERVE_LINEHOLDER", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 10 seconds
print("Script finished successfully!")
Sys.sleep(10)  # Pause for 10 seconds




