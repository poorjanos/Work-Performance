# Analysis of daily workhours per users

# Compute average daily workhours per group
t_ido_csoport <-  t_telj_komb %>% 
  group_by(NAP, F_KISCSOPORT) %>% 
  summarise(MUNKAORA = sum(CKLIDO)/60,
            LETSZAM = length(unique(F_NEV)),
            MUNKAORA_ATL = MUNKAORA/LETSZAM) %>% 
  ungroup() %>% 
  select(NAP, F_KISCSOPORT, MUNKAORA_ATL)
  

# Compute count of days with lower workhours as group avg
t_user_susp <-  t_telj_komb %>% 
  group_by(NAP, F_KISCSOPORT, F_NEV) %>% 
  summarise(MUNKAORA = sum(CKLIDO)/60) %>% 
  ungroup() %>% 
  left_join(t_ido_csoport, by = c("NAP", "F_KISCSOPORT")) %>% 
  mutate(GYANUS_NAP = MUNKAORA < MUNKAORA_ATL) %>% 
  group_by(F_KISCSOPORT, F_NEV) %>% 
  summarize(GYANUS_NAPO_DB = sum(GYANUS_NAP)) %>% 
  ungroup()

write.xlsx(t_user_susp, here::here("Reports", "Workhour_analyis.xlsx"))