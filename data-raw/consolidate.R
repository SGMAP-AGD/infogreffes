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

table_greffes2 <- table_greffes %>%
  filter_(
    .dots = list(
      ~ is.na(date_cloture) == FALSE, 
      ~ is.na(siren) == FALSE
    )
  )

table_greffes2 <- table_greffes2 %>%
  select(denomination, siren, date_cloture, date_depot, chiffre_affaires, resultat, source, duree) %>%
  distinct(siren, date_cloture, chiffre_affaires, resultat, .keep_all = TRUE) 

table_greffes2 <- table_greffes2 %>% 
  mutate(
    source_year = as.numeric(
      sub(pattern = "data\\-raw\\/chiffres\\-cles\\-([[:digit:]]{4})\\.csv", 
          replacement = "\\1", 
          x = source))
  ) 

table_greffes2 <- table_greffes2 %>% 
  group_by(siren, date_cloture) %>%
  filter(source_year == max(source_year))

table_greffes2 <- table_greffes2 %>% 
  ungroup()

table_greffes2 <- table_greffes2 %>%
  group_by(siren) %>%
  mutate(
    duree_interval = ifelse(
      is.na(dplyr::lag(date_cloture)), NA, 
      time_length(
        interval(
          start = dplyr::lag(date_cloture), 
          end = date_cloture), 
        unit = "months")
    ) 
  )

save(table_greffes2, file = "data/table_greffes2.Rda")
