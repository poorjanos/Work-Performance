# Generate agregated performance report for each user
gen_perf_report <- function(df, corr = 1, df_quality, type = "agg") {
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

  