# Print a message indicating that the script has started
print("Script is starting...")

# Install and load the 'librarian' package if not already installed
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian helps to download and load the necessary libraries in one step
librarian::shelf(tidyverse, here, DBI, odbc, padr)

# Define current date and date ranges for filtering pairing data
current_date <- Sys.Date()  # Get today's date
week_prior <- current_date - 3  # Three days before the current date
week_prior_pairing_date <- current_date - 7  # Seven days before the current date

# Extract the bid period from the current date (year-month format)
previous_bid_period <- substr(as.character((current_date)), 1, 7)
raw_date <- Sys.Date()  # Store today's date for later use

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),
                                  Driver = "SnowflakeDSIIDriver",
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                  WAREHOUSE = "DATA_LAKE_READER",
                                  Database = "ENTERPRISE",
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # Replace with your email
                                  authenticator = "externalbrowser")
  print("Database Connected!")  # Success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error message in case of failure
})

# Set the database schema for the current session
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")

# Query the `CT_MASTER_HISTORY` table for pairing data within the date range
q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")
view_masterhistory <- dbGetQuery(db_connection, q_master_history)

# Clean the queried master history data
clean_mh <- view_masterhistory %>% 
  select(CREW_INDICATOR, PAIRING_POSITION, CREW_ID, PAIRING_DATE, BASE, BID_PERIOD, LINE_TYPE, UPDATE_DATE, UPDATE_TIME) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep=" ")) %>%  # Combine update date and time
  group_by(CREW_INDICATOR, PAIRING_POSITION, CREW_ID, PAIRING_DATE) %>%
  filter(update_dt == max(update_dt),  # Keep the latest update only
         LINE_TYPE == "B") %>%  # Filter for Line Type "B"
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", 
                         "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", 
                         "32", "33", "34", "36", "37")) %>% 
  filter(!BASE == "CVG") %>%  # Exclude base "CVG"
  mutate(PAIRING_POSITION = if_else(PAIRING_POSITION == "RO", "FO", PAIRING_POSITION),  # Replace RO with FO for pairing position
         PAIRING_POSITION = if_else(CREW_INDICATOR == "FA", "FA", PAIRING_POSITION)) %>%  # Ensure FA crew indicators are set correctly
  drop_na() %>% 
  ungroup()

# Query the master schedule for the current bid period
q_master_sched <- paste0("select CREW_ID, EQUIPMENT, PAIRING_POSITION, BID_DATE as BID_PERIOD
                         from CT_MASTER_SCHEDULE WHERE BID_DATE ='", previous_bid_period, "';")
raw_ms <- dbGetQuery(db_connection, q_master_sched)

# Clean the master schedule data
clean_ms <- raw_ms %>%
  filter(!CREW_ID %in% c( /* List of crew IDs to exclude */ )) %>% 
  group_by(PAIRING_POSITION, CREW_ID, BID_PERIOD) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  filter(!EQUIPMENT == "33Y") %>%  # Exclude equipment type "33Y"
  mutate(EQUIPMENT = if_else(PAIRING_POSITION == "FA", NA, EQUIPMENT))  # Set EQUIPMENT to NA for flight attendants (FA)

### Database Connection: Connect to `PLAYGROUND` database using Snowflake
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # Replace with your email
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error message in case of failure
})

# Set the database schema for the current session
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")

# Query the reserve utilization data from AA_RESERVE_UTILIZATION
res_utl_q <- paste0("select * from AA_RESERVE_UTILIZATION WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")
raw_reserve_utl <- dbGetQuery(db_connection_pg, res_utl_q)

# Clean the reserve utilization data
clean_reserve_utl <- raw_reserve_utl %>%
  select(PAIRING_POSITION, PAIRING_DATE, BASE, RLV_SCR, EQUIPMENT) %>% 
  rename(R = RLV_SCR) %>%  # Rename RLV_SCR to R
  mutate(BASE = if_else(PAIRING_POSITION %in% c("CA", "FO") & EQUIPMENT == "717", "HAL", BASE),  # Adjust base for equipment 717
         BASE = if_else(PAIRING_POSITION %in% c("CA", "FO") & !EQUIPMENT == "717", "HNL", BASE)) %>%
  select(!EQUIPMENT)

# Merge and summarize master history and reserve utilization data
gold_ms <- clean_mh %>%
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION")) %>%  # Join master history with master schedule
  group_by(PAIRING_POSITION, PAIRING_DATE, BASE, EQUIPMENT, LINE_TYPE) %>%
  reframe(DAILY_COUNT = n()) %>%
  pivot_wider(names_from = LINE_TYPE, values_from = DAILY_COUNT) %>%
  inner_join(clean_reserve_utl, by = c("PAIRING_POSITION", "PAIRING_DATE", "BASE")) %>%
  mutate(reserve_lineholder_perc = round((R/B) * 100, 2))  # Calculate reserve to lineholder percentage

# Query existing reserve utilization data for comparison
present_ut <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_RESERVE_LINEHOLDER")

# Find matching columns between existing and new data
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(gold_ms))

# Filter both datasets to have matching columns for comparison
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- gold_ms %>%
  select(matching_cols)

# Append new records that don't exist in the current data
final_append <- anti_join(final_append_match_cols, match_present_fo, by = join_by(PAIRING_POSITION, PAIRING_DATE, EQUIPMENT, BASE))

# Insert the new records into the AA_RESERVE_LINEHOLDER table
dbAppendTable(db_connection_pg, "AA_RESERVE_LINEHOLDER", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 5 seconds before printing completion message
print("Script finished successfully!")
Sys.sleep(10)  # Pause for 10 seconds at the end