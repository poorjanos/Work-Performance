# 2018H1 final results compute pipeline

# Load required libs
library(config)
library(here)
library(ggplot2)
library(scales)
library(dplyr)
library(lubridate)
library(xlsx)

# Import helper functions --------------------------------------------------------------- 
source(here::here("R", "data_manipulation.R"))

# Define constants ----------------------------------------------------------------------
# Cons: Kontakt credentials
kontakt <-
  config::get("kontakt",
              file = "C:\\Users\\PoorJ\\Projects\\config.yml")

# Cons: accounting period
period  <- "2018H2"

t_komb_suly <- read.csv(here::here("Data",
                                   "Analitika",
                                   paste0("t_sulyok_", period, ".csv")), stringsAsFactors = FALSE)

t_telj_komb <- read.csv(here::here("Data",
                                   "Analitika",
                                   paste0("t_analitika_", period, ".csv")), stringsAsFactors = FALSE)


########################################################################################
# Correction for parallel calls ########################################################
########################################################################################

ivk_to_drop <- read.csv(here::here("Data", "Kontakt", "Hivas_korr_2018H2.csv"), sep = ";")
t_telj_komb <- t_telj_komb %>% filter(!F_IVK %in% ivk_to_drop$F_IVK)


########################################################################################
# Get parameters for quality, cometence and correction #################################
########################################################################################

# Set JAVA_HOME, set max. memory, and load rJava library
Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_171")
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

minoseg <- paste0("select * from t_minoseg_", period)
korr <- "select * from t_bsc_korr"

# Run queries
t_min <- dbGetQuery(jdbcConnection, minoseg)
t_korr <- dbGetQuery(jdbcConnection, korr)

# Close connection
dbDisconnect(jdbcConnection)


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
      # Break up Minbizt
      .$TORZSSZAM %in% c(920578, 920101, 916236) ~ "Welcome",
      .$CSOPORT == "Minõségbiztosítás" ~ "Utánkövetés",
      # Break up op-kiszolg
      .$TORZSSZAM %in% c(945682, 934655, 905358, 934642) ~ "Operatív kiszolgálás partner",
      .$TORZSSZAM %in% c(945682, 904297, 920100, 937351) ~ "Operatív kiszolgálás pu",
      .$TORZSSZAM %in% c(945748, 950265, 920102, 931981) ~ "Operatív kiszolgálás egyeb",
      TRUE ~ .$CSOPORT
    )
  )


#######################################################################################
# Genarate Reports ####################################################################
#######################################################################################

# Compute correction coeff
corr_pct <- t_korr[t_korr$IDOSZAK == period, "PERF_PCT"]
corr_coeff <- ifelse(corr_pct < 0.9, 1 - ((0.9 - corr_pct) / 0.01 * 0.002), 1)

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
    "Final-results",
    paste0("Teljesitmeny_", period, ".xlsx")
  ),
  sheetName = "Gongyolt",
  row.names = FALSE
)
write.xlsx(
  t_telj_lastday,
  here::here(
    "Reports",
    "Final-results",
    paste0("Teljesitmeny_", period, ".xlsx")
  ),
  sheetName = "Tegnap",
  row.names = FALSE,
  append = TRUE
)
write.xlsx(
  t_telj_analitka,
  here::here(
    "Reports",
    "Final-results",
    paste0("Teljesitmeny_", period, ".xlsx")
  ),
  sheetName = "Reszletek",
  row.names = FALSE,
  append = TRUE
)