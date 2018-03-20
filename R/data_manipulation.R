# Add vars to raw IFI df ----------------------------------------------------------------
add_vars_ifi <- function(df){
  df <- df %>% mutate(VEGE = ymd_hms(DATUM),
                      NAP = as.factor(floor_date(VEGE, "day")),
                      HONAP = format.Date(floor_date(as.Date(NAP), "month"), "%Y-%m"),
                      USER_AZON = as.factor(USER_AZON),
                      CEG_SZLASZAM = as.factor(paste0("szla:", CEG_SZLASZAM))) %>% 
              arrange(USER_AZON, VEGE)
  return(df)
}


# Clean IFI df and compute cycle-times for transactions ---------------------------------
comp_ctimes_ifi <- function(df){
  # Generate logs for DATE/USER/ACCOUNT
  nap_collect <- data.frame()
  for (i in levels(df$USER_AZON))
    {
      user_tab <-
        df[df[["USER_AZON"]] == i, ]
      for (j in levels(user_tab$NAP))
      {
        user_tab_nap <- user_tab[user_tab[["NAP"]] == j, ]
        for (k in levels(df$CEG_SZLASZAM))
        {
          user_tab_nap_szla <-
            user_tab_nap[user_tab_nap[["CEG_SZLASZAM"]] == k, ]
          user_tab_nap_szla <-
            mutate(user_tab_nap_szla, KEZDETE = lag(VEGE))
          user_tab_nap_szla <-
            user_tab_nap_szla[complete.cases(user_tab_nap_szla), ]
          user_tab_nap_szla$CKLIDO <-
            as.numeric(difftime(
              user_tab_nap_szla$VEG,
              user_tab_nap_szla$KEZD,
              units = "mins"
            ))
          nap_collect <- rbind(nap_collect, user_tab_nap_szla)
        }
      }
    }
  
  # Clean dataset
  # Delete 0 duration logs (where begin = end)
  nap_collect_lin <- nap_collect %>% 
                        filter(KEZDETE != VEGE) %>% 
                        arrange(USER_AZON, NAP, KEZDETE)
  
  # Delete non-linear logs
  nap_collect_clean <- data.frame()
  for (i in levels(nap_collect_lin$USER_AZON))
  {
    user_tab <- nap_collect_lin[nap_collect_lin[["USER_AZON"]] == i, ]
    for (j in levels(user_tab$NAP))
    {
      user_tab_nap <- user_tab[user_tab[["NAP"]] == j, ]
      user_tab_nap <- mutate(user_tab_nap, CTRL1 = lead(KEZDETE))
      user_tab_nap$FLAG1 <-
        ifelse(
          user_tab_nap$VEGE <= user_tab_nap$CTRL1 |
            is.na(user_tab_nap$CTRL1) == TRUE,
          1,
          0
        )
      user_tab_nap <- user_tab_nap[user_tab_nap$FLAG1 == 1, ]
      nap_collect_clean <- rbind(nap_collect_clean, user_tab_nap)
    }
  }
  
  # Delete suspicious logs: longer than 10 mins and account in (4,6)
  nap_collect_clean$CEG_SZLASZAM <-
    as.character(nap_collect_clean$CEG_SZLASZAM)
  nap_collect_clean <-
    nap_collect_clean[!(
      nap_collect_clean$CEG_SZLASZAM
      %in% c(
        "szla:120010080031242100100004",
        "szla:120010080031244300100006"
      )
      &
        nap_collect_clean$CKLIDO >= 10
    ), ]
  
  # Closing transformations
  nap_collect_clean$CTRL1 <- NULL
  nap_collect_clean$FLAG1 <- NULL
  nap_collect_clean <- nap_collect_clean[c(9, 1:6, 11, 8, 12)]
  
  nap_collect_clean <-
    as.data.frame(lapply(nap_collect_clean, function(x)
      if (is.factor(x) || is.POSIXct(x))
      {
        as.character(x)
      }
      else
      {
        x
      })
      ,
      stringsAsFactors = FALSE)
  
  return(nap_collect_clean)
}


# Generate aggregated performance report for each user -----------------------------------
gen_perf_report <- function(df, corr = 1, df_quality, type = "agg") {
  # Takes as parameter:
  #   1. corr: correction for department performace
  #   2. df_quality: quality and competence scores for corrections
  #   3. type: type of aggregation (cumulative or last day snapshot)
  
  # Generate raw performance metrics
  perf_report <- df %>%
    group_by(CSOPORT, TORZSSZAM, NEV) %>%
    summarise(
      ERINTETT_DB = sum(ERINTETT_DB),
      SULYOZOTT_DB = sum(SULYOZOTT_DB),
      JELENVOLT_MNAP = n_distinct(NAP),
      LEAN_IFI_MNAP = sum(LOGIDO_MM) / 60 / 7
    ) %>%
    mutate(DATUM = as.character(Sys.Date() - 1)) %>%
    select(c(8, 1:7)) %>%
    ungroup()
  
  # Compute expectation
  expectation <- perf_report %>%
    group_by(CSOPORT) %>%
    summarise(NAPI_ELVART_DB = sum(SULYOZOTT_DB) / sum(LEAN_IFI_MNAP)) %>%
    ungroup()
  
  # Compare raw with expected and adjust for quality
  perf_report <-
    left_join(perf_report, expectation, by = "CSOPORT") %>%
    mutate(
      IDOSZAKI_ELVART_DB = LEAN_IFI_MNAP * NAPI_ELVART_DB,
      TELJ_MENNYISEGI_ALAP = SULYOZOTT_DB / IDOSZAKI_ELVART_DB,
      BSC_KORR = corr,
      TELJ_MENNYISEGI = TELJ_MENNYISEGI_ALAP * BSC_KORR
    ) %>%
    left_join(df_quality, by = "TORZSSZAM") %>%
    mutate(TELJ_OSSZESEN = TELJ_MENNYISEGI * TELJ_MINOSEGI) %>%
    arrange(CSOPORT, NEV) %>%
    select(c(1:(length(.) - 2), length(.), length(.) - 1)) %>%
    mutate(
      TELJ_KAT = case_when(.$TELJ_OSSZESEN < 0.9 ~ 0,
                           .$TELJ_OSSZESEN < 1.1 ~ 1,
                           TRUE ~ 2),
      KOMP_KAT = case_when(
        .$CSOPORT %in% c('Utánkövetés', 'Welcome') & .$KOMP < 7 ~ 0,
        .$CSOPORT %in% c('Utánkövetés', 'Welcome') &
          .$KOMP < 10.5 ~ 1,
        .$CSOPORT %in% c('Utánkövetés', 'Welcome') &
          .$KOMP >= 10.5 ~ 2,
        .$KOMP < 6 ~ 0,
        .$KOMP < 9.5 ~ 1,
        TRUE ~ 2
      ),
      BESOROLAS = case_when(
        !is.na(.$KOMP) & (TELJ_KAT + KOMP_KAT < 2) ~ "Bronz", 
        !is.na(.$KOMP) & (TELJ_KAT + KOMP_KAT < 3) ~ "Ezüst",
        !is.na(.$KOMP) & (TELJ_KAT + KOMP_KAT < 4) ~ "Arany",
        !is.na(.$KOMP) & (TELJ_KAT + KOMP_KAT == 4) ~ "Gyémánt",
        TRUE ~ "Nem besorolható"
      )
    )
  
  # Return class only for aggregate types
  if (type == "agg") {
    perf_report <- perf_report %>% select(-TELJ_KAT,-KOMP_KAT)
  } else if (type == "last") {
    perf_report <-
      perf_report %>% select(-KOMP, -TELJ_KAT,-KOMP_KAT,-BESOROLAS)
  }
  
  return(perf_report)
}

  