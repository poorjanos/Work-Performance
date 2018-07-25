# Analysis of daily workhours per users

# Load required libs
library(config)
library(here)
library(ggplot2)
library(scales)
library(dplyr)
library(lubridate)
library(xlsx)

period  <- "2018H2"
t_telj_komb <- read.csv(here::here("Data",
                                   "Analitika",
                                   paste0("t_analitika_", period, ".csv")), stringsAsFactors = FALSE)

# Compute average daily workhours per group
t_ido_csoport <-  t_telj_komb %>% 
  group_by(NAP, F_KISCSOPORT, F_NEV) %>% 
  summarise(MUNKAORA = sum(CKLIDO)/60) %>% 
  ungroup() %>% 
  group_by(NAP, F_KISCSOPORT) %>% 
  summarise(MUNKAORA_ATLAG = mean(MUNKAORA),
            MUNKAORA_SZORAS = sd(MUNKAORA)) %>% 
  ungroup()

# Compute count of days with lower workhours as group avg - 1 sd
t_user_susp <-  t_telj_komb %>% 
  group_by(NAP, F_KISCSOPORT, F_NEV) %>% 
  summarise(MUNKAORA = sum(CKLIDO)/60) %>% 
  ungroup() %>% 
  left_join(t_ido_csoport, by = c("NAP", "F_KISCSOPORT")) %>% 
  mutate(GYANUS_NAP = MUNKAORA < MUNKAORA_ATLAG - MUNKAORA_SZORAS) %>% 
  group_by(F_KISCSOPORT, F_NEV) %>% 
  summarize(GYANUS_NAPO_DB = sum(GYANUS_NAP)) %>% 
  ungroup()

write.xlsx(t_user_susp, here::here("Reports", paste0("Workhour_analysis_", period, ".xlsx")))