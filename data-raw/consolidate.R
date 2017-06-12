library("readr")
library("dplyr")
library("tricky")
library("infogreffes")
library("lubridate")

table_greffes2014_from2014 <- import_infogreffes_from2014(year = 2014, path = "data-raw/chiffres-cles-2014.csv")
table_greffes2014_from2014 <- import_infogreffes_from2014(year = 2014, path = "data-raw/chiffres-cles-2014.csv")
table_greffes2013_from2014 <- import_infogreffes_from2014(year = 2013, path = "data-raw/chiffres-cles-2014.csv")
table_greffes2012_from2014 <- import_infogreffes_from2014(year = 2012, path = "data-raw/chiffres-cles-2014.csv")
table_greffes2015_from2015 <- import_infogreffes_from2015(year = 2015, path = "data-raw/chiffres-cles-2015.csv")
table_greffes2014_from2015 <- import_infogreffes_from2015(year = 2014, path = "data-raw/chiffres-cles-2015.csv")
table_greffes2013_from2015 <- import_infogreffes_from2015(year = 2013, path = "data-raw/chiffres-cles-2015.csv")
table_greffes2016_from2016 <- import_infogreffes_from2016(year = 2016, path = "data-raw/chiffres-cles-2016.csv")
table_greffes2015_from2016 <- import_infogreffes_from2016(year = 2015, path = "data-raw/chiffres-cles-2016.csv")
table_greffes2014_from2016 <- import_infogreffes_from2016(year = 2014, path = "data-raw/chiffres-cles-2016.csv")

table_greffes <- bind_rows(
  table_greffes2014_from2014, 
  table_greffes2013_from2014, 
  table_greffes2012_from2014, 
  table_greffes2015_from2015, 
  table_greffes2014_from2015, 
  table_greffes2013_from2015,
  table_greffes2016_from2016, 
  table_greffes2015_from2016, 
  table_greffes2014_from2016) 

rm(list = ls(pattern = "table\\_greffes20*"))

save(table_greffes, file = "data/table_greffes.Rda")

load("data/table_greffes.Rda")

clean_table_greffes <- function(tbl) {
  tbl %>%
    dplyr::filter_(
      .dots = list(
        ~ is.na(date_cloture) == FALSE, 
        ~ is.na(siren) == FALSE
      )
    ) %>% 
    dplyr::select_(
      .dots = list(~ denomination, ~ siren, ~ date_cloture, ~ date_depot, ~ chiffre_affaires, ~ resultat, ~ source, ~ duree)
      ) %>%
    dplyr::distinct_(
      .dots = list(~ siren, ~ date_cloture, ~ chiffre_affaires, ~ resultat), 
      .keep_all = TRUE
      ) %>%
  dplyr::mutate_(
    .dots = list(
      "source_year" = ~ as.numeric(
        sub(pattern = "data\\-raw\\/chiffres\\-cles\\-([[:digit:]]{4})\\.csv", 
            replacement = "\\1", 
            x = source)
        )
      )
    ) %>%
    dplyr::group_by_(.dots = list(~ siren, ~ date_cloture)) %>%
    dplyr::filter_(.dots = ~ source_year == max(source_year)) %>% 
    dplyr::ungroup() %>%
    dplyr::group_by_(.dots = list(~ siren)) %>%
    dplyr::mutate_(
      .dots = list(
        "duree_interval" = ~ ifelse(
          is.na(dplyr::lag(date_cloture)), 
          NA,
          lubridate::time_length(
            lubridate::interval(
              start = dplyr::lag(date_cloture), 
              end = date_cloture
              ), 
          unit = "months"
          )
        ) 
      )
    ) %>% 
    dplyr::ungroup()
  }

# clean_table_greffes(tbl = table_greffes) 

table_greffes2 <- clean_table_greffes(tbl = table_greffes) 
save(table_greffes2, file = "data/table_greffes2.Rda")
