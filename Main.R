# Procedure to compute performance metrics for users. 
# Steps:
#   Prepare env
#   Extract data from portfolio and financial system
#   Transform data to get uniform task representation
#   Compute weights to normalize for different tasks
#   Compute weighted performance
#   Generate reports
#   Save results


#########################################################################################
# Prepare env ###########################################################################
#########################################################################################

# Redirect stdout to logfile
scriptLog <- file("scriptLog", open = "wt")
sink(scriptLog, type = "message")

# Load required libs
library(config)
library(here)
library(ggplot2)
library(scales)
library(dplyr)
library(lubridate)
library(xlsx)

# Quit if sysdate == weekend ------------------------------------------------------------
stopifnot(!(strftime(Sys.Date(),'%u') == 1 | strftime(Sys.Date(),'%u') == 7))

# Import helper functions --------------------------------------------------------------- 
source(here::here("R", "data_manipulation.R"))

# Define constants ----------------------------------------------------------------------
# Cons: accounting period
period  <-
  ifelse(month(floor_date(Sys.Date(), unit = "month")) < 7,
         paste0(year(floor_date(Sys.Date(
           
         ), unit = 'year')), "H1"),
         paste0(year(floor_date(Sys.Date(
           
         ), unit = 'year')), "H2"))

# Cons: Kontakt credentials
kontakt <-
  config::get("kontakt",
              file = "C:\\Users\\PoorJ\\Projects\\config.yml")

# Cons: IFI credentials
ifi <-
  config::get("ifi",
              file = "C:\\Users\\PoorJ\\Projects\\config.yml")

# Create dirs (dir.create() does not crash when dir already exists) ---------------------
dir.create(here::here("Data"), showWarnings = FALSE)
dir.create(here::here("Data", "Analitika"), showWarnings = FALSE)
dir.create(here::here("Data", "IFI"), showWarnings = FALSE)
dir.create(here::here("Data", "Kontakt"), showWarnings = FALSE)
dir.create(here::here("Data", "Dashboard"), showWarnings = FALSE)
dir.create(here::here("Reports"), showWarnings = FALSE)
dir.create(here::here("SQL"), showWarnings = FALSE)
dir.create(here::here("Utils"), showWarnings = FALSE)


########################################################################################
# Data Extraction ######################################################################
########################################################################################

# Set JAVA_HOME, set max. memory, and load rJava library
Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_60")
options(java.parameters = "-Xmx2g")
library(rJava)

# Output Java version
.jinit()
print(.jcall("java/lang/System", "S", "getProperty", "java.version"))

# Load RJDBC library
library(RJDBC)

# Create connection driver
jdbcDriver <-
  JDBC(driverClass = "oracle.jdbc.OracleDriver",
       classPath = "C:\\Users\\PoorJ\\Desktop\\ojdbc7.jar")


# Extract KONTAKT data ----------------------------------------------------------------
# Open connection
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = kontakt$server,
    user = kontakt$uid,
    password = kontakt$pwd
  )

# Get SQL scripts
readQuery <-
  function(file)
    paste(readLines(file, warn = FALSE), collapse = "\n")
user_name_group <-
  readQuery(here::here(
    "Utils",
    "user_name_group.sql"
  ))
al_il_darabok <- "select * from t_al_il_telj_kimenet"
al_il_sulyok <- "select * from t_al_il_suly_kimenet"
minoseg <- paste0("select * from t_minoseg_", period)
korr <- "select * from t_bsc_korr"

# Run queries
t_al_il_feladat_suly <- dbGetQuery(jdbcConnection, al_il_sulyok)
t_al_il_telj <- dbGetQuery(jdbcConnection, al_il_darabok)
t_user_data <- dbGetQuery(jdbcConnection, user_name_group)
t_min <- dbGetQuery(jdbcConnection, minoseg)
t_korr <- dbGetQuery(jdbcConnection, korr)

# Close connection
dbDisconnect(jdbcConnection)


# Extract IFI data --------------------------------------------------------------------
# Open connection
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = ifi$server,
    user = ifi$uid,
    password = ifi$pwd
  )

# Get SQL scripts
ifi_napi_darabok <-
  readQuery(here::here(
    "SQL",
    "ifi_napi_darabok.sql"
  ))

# Run queries
t_ifi_napi_darabok <- dbGetQuery(jdbcConnection, ifi_napi_darabok)

# Close connection
dbDisconnect(jdbcConnection)


########################################################################################
# Data Transformation ##################################################################
########################################################################################

# Transform KONTAKT data --------------------------------------------------------------
t_al_il_feladat_suly[is.na(t_al_il_feladat_suly$F_OKA), "F_OKA"] <-
  "Egyéb"
t_al_il_telj[is.na(t_al_il_telj$F_OKA), "F_OKA"] <- "Egyéb"


# Transform IFI data -------------------------------------------------------------------
t_ifi_napi_darabok <- add_vars_ifi(t_ifi_napi_darabok)

# TEMPORARY RAW HISTORY MANAGEMENT; DEPRECATE ONCE ORACLE JOBS EFFECTIVE
# Load raw history from local storage
ifi_history_raw <-
  read.csv(
    here::here(
      "Data",
      "IFI",
      "ifi_history_raw.csv"
    ),
    stringsAsFactors = FALSE
  )

# Append current to raw history
ifi_history_raw <- rbind(ifi_history_raw, t_ifi_napi_darabok)
ifi_history_raw <-
  arrange(ifi_history_raw, USER_AZON, NAP, CEG_SZLASZAM, VEGE)

# Save raw history to local storage
write.csv(
  ifi_history_raw,
  here::here(
    "Data",
    "IFI",
    "ifi_history_raw.csv"
  ),
  row.names = FALSE
)

# Compute IFI cycle-times
t_ifi_clean <- comp_ctimes_ifi(t_ifi_napi_darabok)

# Merge and save
# Load history from local storage
ifi_history <-
  read.csv(
    here::here(
      "Data",
      "IFI",
      "ifi_history.csv"
    ),
    stringsAsFactors = FALSE
  )

# Append current to history
ifi_history <- rbind(ifi_history, t_ifi_clean)
ifi_history <-
  arrange(ifi_history, USER_AZON, NAP, CEG_SZLASZAM, KEZDETE)

# Save history to local storage
write.csv(
  ifi_history,
  here::here(
    "Data",
    "IFI",
    "ifi_history.csv"
  ),
  row.names = FALSE
)

# Append current to db table
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = kontakt$server,
    user = kontakt$uid,
    password = kontakt$pwd
  )

try(dbWriteTable(
  jdbcConnection,
  name = "T_IFI_HISTORY",
  value = t_ifi_clean,
  overwrite = FALSE,
  append = TRUE
))

dbDisconnect(jdbcConnection)


#########################################################################################
# Compute Weights #######################################################################
#########################################################################################

# Create IFI aggregated weight table to merge to KONTAKT weight data
ifi_history_tr <-
  as.data.frame(ifi_history[ymd(ifi_history$NAP) >= Sys.Date() - 180, ])

ifi_history_agg <- ifi_history_tr %>%
  mutate(F_LEAN_TIP = 'IFI',
         TEVEKENYSEG = 'IFI díjazonosítás') %>%
  group_by(F_LEAN_TIP, TEVEKENYSEG, F_OKA = CEG_SZLASZAM, KIMENET = "Lezar") %>%
  summarize(
    MEDIAN = median(CKLIDO),
    TEV_DB = as.numeric(length(FORGTET_AZON)),
    KEZELT_DB = as.numeric(length(FORGTET_AZON))
  ) %>%
  ungroup()

# Merge Kontakt & IFI and compute weights
t_komb_suly <- rbind(t_al_il_feladat_suly, ifi_history_agg)

# Compute base then all weights
bazis <-
  t_komb_suly[t_komb_suly$TEVEKENYSEG == "LAKÁS sikertelen KPM adatellenõrzés" &
                t_komb_suly$F_OKA == "Normál" &
                t_komb_suly$KIMENET == "Lezar"
              , "MEDIAN"]

t_komb_suly <-
  t_komb_suly %>% mutate(BAZIS = bazis, SULY = MEDIAN / bazis)

# Save weights to local storage and db table
write.csv(
  t_komb_suly,
  here::here(
    "Data",
    "Analitika",
    paste0("t_sulyok_", period, ".csv")
  ),
  row.names = FALSE
)

# Overwrite db table
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = kontakt$server,
    user = kontakt$uid,
    password = kontakt$pwd
  )

try(dbWriteTable(
  jdbcConnection,
  name = paste0("T_SULYOK_", period),
  value = t_komb_suly,
  overwrite = TRUE,
  append = FALSE
))

dbDisconnect(jdbcConnection)


########################################################################################
# Compute performance ##################################################################
########################################################################################

# KONAKT -------------------------------------------------------------------------------
# Compute unit based analitics (in case of KONTAKT accounting perid is queried by original SQL)
t_telj_kontakt <-
  mutate(t_al_il_telj, NAP = as.Date(F_INT_BEGIN)) %>%
  select(
    NAP,
    F_LEAN_TIP,
    F_IVK,
    TORZSSZAM,
    KEZDETE = F_INT_BEGIN,
    VEGE = F_INT_END,
    TEVEKENYSEG,
    F_OKA,
    KIMENET,
    CKLIDO
  )

# IFI ----------------------------------------------------------------------------------
# Get accounting period start (in case of KONTAKT history table is queried by origonal SQL)
# -> needs to be truncated here
if (month(Sys.Date()) < 7) {
  period_start <-  make_date(year(Sys.Date()), 1, 1)
} else{
  period_start <- make_date(year(Sys.Date()), 7, 1)
}

# Compute unit based analitics
t_telj_ifi <-
  ifi_history[ymd(ifi_history$NAP) >= period_start, ] %>%
  mutate(
    F_LEAN_TIP = 'IFI',
    TEVEKENYSEG = 'IFI díjazonosítás',
    KIMENET = 'Lezar',
    F_IVK = paste0(SZLAFORG_FEJ_AZON, "_", FORGTET_AZON, "_",
                   AZON_FORGTET_AZON)
  ) %>%
  select(NAP,
         F_LEAN_TIP,
         F_IVK,
         TORZSSZAM,
         KEZDETE,
         VEGE,
         TEVEKENYSEG,
         F_OKA = CEG_SZLASZAM,
         KIMENET,
         CKLIDO)


# Merge KONTAKT and IFI ----------------------------------------------------------------
t_telj_komb <- rbind(t_telj_kontakt, t_telj_ifi)

# Add group and name
t_telj_komb <-
  left_join(t_telj_komb, t_user_data, by = c("TORZSSZAM" = "F_TORZSSZAM"))

# Save analytics to local storage
write.csv(
  t_telj_komb,
  here::here(
    "Data",
    "Analitika",
    paste0("t_analitika_", period, ".csv")
  ),
  row.names = FALSE
)

# Aggregate and add weights ------------------------------------------------------------
t_telj_komb_agg <- t_telj_komb %>%
  group_by(
    NAP,
    CSOPORT = F_KISCSOPORT,
    TORZSSZAM,
    NEV = F_NEV,
    LEAN_TIP = F_LEAN_TIP,
    TEVEKENYSEG,
    OKA = F_OKA,
    KIMENET
  ) %>%
  summarize(ERINTETT_DB = length(F_IVK),
            LOGIDO_MM = sum(CKLIDO)) %>%
  left_join(
    t_komb_suly[c("F_LEAN_TIP", "TEVEKENYSEG", "F_OKA", "KIMENET", "SULY")],
    by = c(
      "LEAN_TIP" = "F_LEAN_TIP",
      "TEVEKENYSEG" = "TEVEKENYSEG",
      "OKA" = "F_OKA",
      "KIMENET" = "KIMENET"
    )
  ) %>%
  mutate(SULYOZOTT_DB = ERINTETT_DB *
           SULY) %>% 
  ungroup()

# Remove users and recode groups--------------------------------------------------------
t_telj_komb_agg <- t_telj_komb_agg %>%
  filter(!TORZSSZAM %in% c(110398, 913128)) %>%
  mutate(
    CSOPORT = case_when(
      .$TORZSSZAM %in% c(920578, 920101, 916236) ~ "Welcome",
      .$CSOPORT == "Minõségbiztosítás" ~ "Utánkövetés",
      TRUE ~ .$CSOPORT
    )
  )


#######################################################################################
# Genarate Reports ####################################################################
#######################################################################################

# Compute correction coeff
corr_pct <- t_korr[t_korr$IDOSZAK == period, "PERF_PCT"]
corr_coeff <- 1 - ((0.9 - corr_pct) / 0.01 * 0.002)

# Generate final rolling report per user ----------------------------------------------
t_telj_final <- gen_perf_report(t_telj_komb_agg, corr_coeff, t_min, type = "agg")

# Generate report for day before per user ---------------------------------------------
t_telj_lastday <-
  t_telj_komb_agg[t_telj_komb_agg$NAP == max(t_telj_komb_agg$NAP), ]

t_telj_lastday <- gen_perf_report(t_telj_lastday, corr_coeff, t_min, type = "last")

# Generate aggregated analytics per user ----------------------------------------------
t_telj_csoport <-
  group_by(
    t_telj_komb,
    CSOPORT = F_KISCSOPORT,
    LEAN_TIP = F_LEAN_TIP,
    TEVEKENYSEG,
    OKA = F_OKA,
    KIMENET
  ) %>%
  summarize(
    ERINTETT_DB = length(F_IVK),
    LOGIDO_MM = sum(CKLIDO) / 60 / 7,
    CSOPORT_CKLIDO_ATLAG = mean(CKLIDO),
    CSOPORT_CKLIDO_MEDIAN = median(CKLIDO)
  ) %>%
  left_join(
    t_komb_suly[c("F_LEAN_TIP", "TEVEKENYSEG",
                  "F_OKA", "KIMENET", "SULY")],
    by = c(
      "LEAN_TIP" = "F_LEAN_TIP",
      "TEVEKENYSEG" =
        "TEVEKENYSEG",
      "OKA" = "F_OKA",
      "KIMENET" = "KIMENET"
    )
  ) %>%
  mutate(SULYOZOTT_DB = ERINTETT_DB *
           SULY,
         CSOPORT_NAPI_DB = ERINTETT_DB *
           SULY / LOGIDO_MM)


t_telj_analitka <-
  group_by(
    t_telj_komb,
    CSOPORT = F_KISCSOPORT,
    TORZSSZAM,
    NEV = F_NEV,
    LEAN_TIP = F_LEAN_TIP,
    TEVEKENYSEG,
    OKA = F_OKA,
    KIMENET
  ) %>%
  summarize(
    ERINTETT_DB = length(F_IVK),
    LOGIDO_MM = sum(CKLIDO) / 60 / 7,
    USER_CKLIDO_ATLAG = mean(CKLIDO),
    USER_CKLIDO_MEDIAN = median(CKLIDO)
  ) %>%
  left_join(
    t_komb_suly[c("F_LEAN_TIP", "TEVEKENYSEG",
                  "F_OKA", "KIMENET", "SULY")],
    by = c(
      "LEAN_TIP" = "F_LEAN_TIP",
      "TEVEKENYSEG" = "TEVEKENYSEG",
      "OKA" = "F_OKA",
      "KIMENET" = "KIMENET"
    )
  ) %>%
  mutate(SULYOZOTT_DB = ERINTETT_DB * SULY,
         USER_NAPI_DB = ERINTETT_DB * SULY /
           LOGIDO_MM) %>%
  left_join(
    select(
      t_telj_csoport,
      CSOPORT,
      LEAN_TIP,
      TEVEKENYSEG,
      OKA,
      KIMENET,
      CSOPORT_CKLIDO_ATLAG,
      CSOPORT_CKLIDO_MEDIAN,
      CSOPORT_NAPI_DB
    ),
    by = c(
      "CSOPORT" = "CSOPORT",
      "LEAN_TIP" = "LEAN_TIP",
      "TEVEKENYSEG" = "TEVEKENYSEG",
      "OKA" = "OKA",
      "KIMENET" = "KIMENET"
    )
  )

t_telj_analitka <-
  t_telj_analitka %>% select(
    CSOPORT,
    TORZSSZAM,
    NEV,
    LEAN_TIP,
    TEVEKENYSEG,
    OKA,
    KIMENET,
    SULY,
    ERINTETT_DB,
    SULYOZOTT_DB,
    USER_CKLIDO_ATLAG,
    CSOPORT_CKLIDO_ATLAG,
    USER_CKLIDO_MEDIAN,
    CSOPORT_CKLIDO_MEDIAN,
    USER_NAPI_DB,
    CSOPORT_NAPI_DB
  ) %>%
  arrange(CSOPORT, NEV, LEAN_TIP, TEVEKENYSEG, OKA, KIMENET)


########################################################################################
# Save results as xlsx to .\\Reports ###################################################
########################################################################################

# Save xlsx
attributes(t_telj_final)$class <- c("data.frame")
attributes(t_telj_lastday)$class <- c("data.frame")
attributes(t_telj_analitka)$class <- c("data.frame")

write.xlsx(
  t_telj_final,
  here::here(
    "Reports",
    paste0("Teljesitmeny_", Sys.Date() - 1, ".xlsx")
  ),
  sheetName = "Gongyolt",
  row.names = FALSE
)
write.xlsx(
  t_telj_lastday,
  here::here(
    "Reports",
    paste0("Teljesitmeny_", Sys.Date() - 1, ".xlsx")
  ),
  sheetName = "Tegnap",
  row.names = FALSE,
  append = TRUE
)
write.xlsx(
  t_telj_analitka,
  here::here(
    "Reports",
    paste0("Teljesitmeny_", Sys.Date() - 1, ".xlsx")
  ),
  sheetName = "Reszletek",
  row.names = FALSE,
  append = TRUE
)


# Save dashboard input to local storage and db table
to_gongy <- t_telj_final %>% select(-KOMP, -BESOROLAS) %>% ungroup()
to_napi <- t_telj_lastday %>% ungroup()

gongy <-
  read.csv(
    here::here(
      "Data",
      "Dashboard",
      "Telj_gongyolt.csv"
    ),
    stringsAsFactors = FALSE
  )
napi <-
  read.csv(
    here::here(
      "Data",
      "Dashboard",
      "Telj_napi.csv"
    ),
    stringsAsFactors = FALSE
  )

# Recode NA to 999 as DBI::dbSendUpdate randomly fails on NA/NaN
to_gongy[is.na(to_gongy)] <- 999  
to_napi[is.na(to_napi)] <- 999  

gongy <- rbind(gongy, to_gongy)
napi <- rbind(napi, to_napi)

write.csv(
  gongy,
  here::here(
    "Data",
    "Dashboard",
    "Telj_gongyolt.csv"
  ),
  row.names = FALSE
)
write.csv(
  napi,
  here::here(
    "Data",
    "Dashboard",
    "Telj_napi.csv"
  ),
  row.names = FALSE
)

# Append dashboard input to db
jdbcConnection <-
  dbConnect(
    jdbcDriver,
    url = kontakt$server,
    user = kontakt$uid,
    password = kontakt$pwd
  )

try(dbWriteTable(
  jdbcConnection,
  name = "T_GONGY_TELJ",
  value = to_gongy,
  overwrite = FALSE,
  append = TRUE
))

try(dbWriteTable(
  jdbcConnection,
  name = "T_NAPI_TELJ",
  value = to_napi,
  overwrite = FALSE,
  append = TRUE
))

dbDisconnect(jdbcConnection)


# Redirect stdout back to console
sink(type = "message")
close(scriptLog)
