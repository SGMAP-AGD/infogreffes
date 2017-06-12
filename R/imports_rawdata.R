#' Import data from 2014 file
#'
#' @param year the year to be extracted
#' @param path the path to the source file
#'
#' @return a tibble
#' @export
#'
#' @examples
#' 
#' \dontrun{
#' import_infogreffes_from2014(year = 2013, path = "raw-data/chiffres-cles-2014.csv")
#' import_infogreffes_from2014(year = 2014, path = "raw-data/chiffres-cles-2014.csv")
#' import_infogreffes_from2014(year = 2012, path = "raw-data/chiffres-cles-2014.csv")
#' }
#' 
import_infogreffes_from2014 <- function(year, path) {
  
  list_variables <- list()
  
  list_variables$'2012' <- list(~denomination, ~siren, 
                                ~date_de_cloture1, ~date_de_depot1, ~millesime1, 
                                ~ca_2012, ~resultat_2012,  ~effectifs_2012)
  list_variables$'2013' <- list(~denomination, ~siren, 
                                ~date_de_cloture0, ~date_de_depot0, ~millesime0, 
                                ~ca_2013, ~resultat_2013,  ~effectifs_2013)
  list_variables$'2014' <- list(~denomination, ~siren, 
                                ~date_de_cloture, ~date_de_depot, ~millesime, 
                                ~ca_2014, ~resultat_2014,  ~effectifs_2014)
  
  variable_names <- c("denomination", "siren", "date_cloture", "date_depot", "millesime", "chiffre_affaires", "resultat", "effectif")
  
  list_variables <- lapply(list_variables, setNames, variable_names)
  
  readr::read_csv2(
    file = path, 
    progress = FALSE, 
    locale = locale(decimal_mark = ",")
    ) %>% 
    dplyr::select_(
      .dots = magrittr::extract2(
        list_variables, as.character(year)
      )
    ) %>%
    dplyr::mutate_(.dots = list("source" = ~ path))

}

#' Imports data from 2015 file
#'
#' @param year the year to be extracted 
#' @param path the path to the source file
#'
#' @return a tibble
#' @export
#'
#' @examples
#' 
#' \dontrun{
#' import_infogreffes_from2015(year = 2013, path = "raw-data/chiffres-cles-2015.csv")
#' import_infogreffes_from2015(year = 2014, path = "raw-data/chiffres-cles-2015.csv")
#' import_infogreffes_from2015(year = 2015, path = "raw-data/chiffres-cles-2015.csv")
#' }
#' 
import_infogreffes_from2015 <- function(year, path) {
  
  list_variables <- list()
  list_variables$'2013' <- list(~denomination, ~siren, 
                                ~date_de_cloture_exercice_3, 
                                ~duree_3, ~millesime_3, ~ca_3, 
                                ~resultat_3, ~effectif_3)
  list_variables$'2014' <- list(~denomination, ~siren, 
                                ~date_de_cloture_exercice_2, 
                                ~duree_2, ~millesime_2, ~ca_2, 
                                ~resultat_2, ~effectif_2)
  list_variables$'2015' <- list(~denomination, ~siren, 
                                ~date_de_cloture_exercice_1, 
                                ~duree_1, ~millesime_1, ~ca_1, 
                                ~resultat_1, ~effectif_1)
  names_variables_2015 <- c("denomination", "siren", "date_cloture", "duree", 
                            "millesime", "chiffre_affaires", 
                            "resultat", "effectif")
  list_variables <- lapply(X = list_variables, setNames, names_variables_2015)  
  
  readr::read_csv2(
    file = path, 
    progress = FALSE, 
    col_types = cols(
      siren = col_character()
      ), 
    locale = locale(decimal_mark = ",")
    ) %>% 
  dplyr::select_(
    .dots = magrittr::extract2(
    list_variables, as.character(year)
    )
  ) %>% 
  dplyr::mutate_(.dots = list("source" = ~ path))
  
}

#' Import data from the 2016 file
#'
#' @param year the year to be extracted 
#' @param path the path to the source file
#'
#' @return a tibble
#' @export
#'
#' @examples
#' 
#' \dontrun{
#' import_infogreffes_from2016(year = 2016, source_file = "raw-data/chiffres-cles-2016.csv")
#' }
#' 
import_infogreffes_from2016 <- function(year, path) {
  
  list_variables <- list()
  
  list_variables$'2016' <- list(~denomination, ~siren, ~millesime_1, 
                                ~date_de_cloture_exercice_1, ~duree_1, 
                                ~ca_1, ~resultat_1, ~effectif_1)
  list_variables$'2015' <- list(~denomination, ~siren, ~millesime_2, 
                                ~date_de_cloture_exercice_2, ~duree_2, 
                                ~ca_2, ~resultat_2, ~effectif_2)
  list_variables$'2014' <- list(~denomination, ~siren, ~millesime_3, 
                                ~date_de_cloture_exercice_3, ~duree_3, 
                                ~ca_3, ~resultat_3, ~effectif_3)
  names_variables <- c("denomination", "siren", "millesime", "date_cloture", 
                       "duree", "chiffre_affaires", "resultat", "effectif")
  
  list_variables <- lapply(list_variables, setNames, names_variables)
  
  readr::read_csv2(
    file = path, 
    progress = FALSE, 
    col_types = cols(
      siren = col_character()
    ), 
  locale = locale(decimal_mark = ",")
  ) %>%
  dplyr::select_(
      .dots = magrittr::extract2(
        list_variables, as.character(year)
      )
    ) %>%
  dplyr::mutate_(
    .dots = list("source" = ~ path)
    )
}
