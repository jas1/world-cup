library(here)
library(rebus)
library(purrr)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(rebus)
library(lubridate)


# funciones ---------------------------------------------------------------

parse_copas_extraer_grupos <- function(datos_copa){
    texto <- datos_copa
    grupos_regex <- "Group" %R% one_or_more(rebus::BLANK) %R% one_or_more(rebus::ALNUM)%R% one_or_more(rebus::BLANK)%R% rebus::PIPE
    grupos <- texto[texto %>% stringr::str_detect(grupos_regex)]
    grupos_df <- tibble::enframe(grupos) %>% 
        tidyr::separate(value,into=c('grupo','paises'),sep='\\|') %>% 
        mutate(grupo=stringr::str_trim(grupo)) %>% 
        mutate(paises=stringr::str_trim(paises)) %>% 
        tidyr::separate(paises,into=c("pais1","pais2","pais3","pais4")) %>% 
        tidyr::gather(gr,pais,pais1,pais2,pais3,pais4) %>% 
        filter(!is.na(pais)) %>% 
        select(-gr,-grupo) %>% 
        rename(grupo=name)
    grupos_df
}


extraer_partidos <-  function(datos_copa){
    texto <- datos_copa
    
    no_comentarios <- texto[stringr::str_starts(string=texto,pattern = "#",negate = TRUE)]
    partidos <- no_comentarios[stringr::str_detect(string = no_comentarios,pattern = "@")] %>% stringr::str_trim()
    
    partidos
}
parse_partidos <- function(partidos_data,listado_indices){
    
    regex_equipos_fechas <- rebus::one_or_more(rebus::ALPHA) %R%
        rebus::optional(rebus::one_or_more(rebus::BLANK)) %R%
        rebus::one_or_more(rebus::WRD)
    
    regex_puntos <- rebus::one_or_more(rebus::DGT) %R% '-' %R% rebus::one_or_more(rebus::DGT)
    
    regex_parentesis <- rebus::OPEN_PAREN %R% 
        rebus::one_or_more(rebus::negated_char_class(rebus::ALPHA)) %R% 
        rebus::CLOSE_PAREN

    # indice_para_fake <- 2    
    # partidos_data <- wc_cups_data$partidos_crudo[indice_para_fake]
    # partidos_data <- partidos_data[[1]]
    # listado_indices <- listado_indices_split[indice_para_fake]
    # listado_indices <- listado_indices[[1]]
    # partidos_full_crudo <- data.frame(partidos,goles,stringsAsFactors = FALSE) %>% as_tibble() 
    # partidos_full_crudo <- data.frame(partidos=partidos_data[[1]],stringsAsFactors = FALSE) %>% as_tibble()
    partidos_full_crudo <- data.frame(partidos=partidos_data,stringsAsFactors = FALSE) %>% as_tibble() 
    partidos_estadio_ciudad <- partidos_full_crudo %>% 
        # parseo de partidos
        tidyr::separate(partidos,sep="@",into=c("partido","estadio")) %>% 
        # locacion
        tidyr::separate(estadio,sep=",",into=c("estadio","ciudad")) %>% 
        mutate(estadio=str_trim(estadio)) %>% 
        mutate(ciudad=str_trim(ciudad)) 
    
    partidos_pos_fecha <- partidos_estadio_ciudad %>% 
        #equipos
        # select(partido) %>% 
        mutate(partido_orden=stringr::str_trim(stringr::str_sub(partido,listado_indices[1],listado_indices[2])))%>% 
        mutate(fecha=stringr::str_trim(stringr::str_sub(partido,listado_indices[3],listado_indices[4])))
        
    # # Austria        3–2 a.e.t. (1-1, 1-1) France   
    # regex_equipos_2 <- rebus::one_or_more(rebus::WRD) %R% rebus::optional(rebus::BLANK %R% rebus::one_or_more(rebus::WRD))
    # 
    
    # t1 <- stringr::str_sub("(1) 27 May   Sweden         3–2 (1-1)  Argentina      ",start = listado_indices[4]+1)
    # t2 <- stringr::str_sub("(17) 10 June   Italy 2–1 a.e.t. (1-1, 0-0)  Czechoslovakia  ",start = listado_indices[4]+1)
    # t3 <- stringr::str_sub("(7) 27 May   Italy          7–1 (3-0)  United States  ",start = listado_indices[4]+1)
    # str_replace_all(t1,rebus::DGT,"")
    # str_replace_all("-","")
    # 
    # str_replace_all(t2,"a\\.e\\.t\\.","") %>% str_replace_all("[^[:alpha:][:space:]]","")
    # 
    # stringr::str_extract_all(str_replace_all(t1,"[^[:alpha:][:space:]]",""),regex_equipos_fechas)
    # stringr::str_extract_all(str_replace_all(t2,"[^[:alpha:][:space:]]",""),regex_equipos_fechas)
    # stringr::str_extract_all(str_replace_all(t3,"[^[:alpha:][:space:]]",""),regex_equipos_fechas)
    # stringr::str_extract(t1,regex_puntos_parentesis)
   # stringr::str_replace_all(t2,regex_parentesis,"") %>% stringr::str_extract_all(one_or_more(DGT))
   # stringr::str_replace_all(t1,regex_parentesis,"") %>% stringr::str_extract_all(one_or_more(DGT))
   # stringr::str_replace_all(t2,regex_parentesis,"")  %>% stringr::str_extract_all(one_or_more(DGT))
   # stringr::str_replace_all(t3,regex_parentesis,"")  %>% stringr::str_extract_all(one_or_more(DGT))
    
    
    partidos_nombre_equipos <- partidos_pos_fecha %>% 
        mutate(equipos=stringr::str_trim(stringr::str_sub(partido,start = listado_indices[4]+1))) %>%
        mutate(equipos=str_replace_all(equipos,"a\\.e\\.t\\.","")) %>% 
        mutate(equipos=str_replace_all(equipos,"[^[:alpha:][:space:]]","")) %>% 
        
        mutate(equipos=stringr::str_extract_all(equipos,regex_equipos_fechas)) %>% 
        mutate(equipos=purrr::map(equipos,.f=function(x){paste0(collapse=";",x)})) %>% tidyr::unnest(equipos) %>% #View()
        tidyr::separate(equipos,sep=";",into=c("nombre_eq_1","nombre_eq_2")) %>% 
        mutate(nombre_eq_1=stringr::str_trim(nombre_eq_1)) %>% #pull(equipos)
        mutate(nombre_eq_2=stringr::str_trim(nombre_eq_2)) #pull(equipos)
    
    partidos_resultados <- partidos_nombre_equipos %>% 
        mutate(resultado=stringr::str_trim(stringr::str_sub(partido,start = listado_indices[4]+1))) %>%
        mutate(resultado=stringr::str_replace_all(resultado,regex_parentesis,"")) %>% 
        mutate(resultado=stringr::str_extract_all(resultado,rebus::one_or_more(rebus::DGT))) %>% 
        mutate(resultado=purrr::map(resultado,.f=function(x){paste0(collapse=";",x)})) %>% tidyr::unnest(resultado) %>% #View()
        tidyr::separate(resultado,sep=";",into=c("goles_eq_1","goles_eq_2")) %>% 
 
    partidos_filtro_final<- partidos_resultados %>% 
        select(-partido,partido_orden,fecha,nombre_eq_1,nombre_eq_2,goles_eq_1,goles_eq_2)
    
    partidos_filtro_final
}
# Armar origenes de datos ------------------------------------------------------

wc_path_dirs_regex <- rebus::DGT %R% rebus::DGT %R% rebus::DGT %R% rebus::DGT %R% "--"
wc_data <- list.files(path = here::here(),pattern = wc_path_dirs_regex) %>% 
    tibble::enframe() %>% 
    rename(dir_name = value) %>% 
    mutate(file_wc_data= purrr::map(dir_name,.f = function(x){
        # read_cup_data <- here::here(x,"cup.txt")
        # read_cup_data
        "cup.txt"
    }) ) %>% 
    tidyr::unnest(file_wc_data) %>%
    mutate(file_wc_teams_data = purrr::map(dir_name,.f = function(x){
        read_cup_data <- here::here(x,"squads")
        list.files(read_cup_data)
    }) ) %>% 
    tidyr::unnest(file_wc_teams_data)

# obtener los datos de los archivos ------------------------------------------------------

wc_cups_data <- wc_data %>% 
    count(dir_name,file_wc_data,name = 'dato_cant_equipos') %>% 
    mutate(current_path =  here::here(dir_name,file_wc_data)) %>% 
    mutate(wc_cup_texto = purrr::map(current_path,readr::read_lines)) %>% 
    select(-file_wc_data,-current_path)

# wc_parsed_cups_data %>% tidyr::unnest(wc_cup_texto)

wc_teams_by_cup_data <- wc_data %>% 
    mutate(team_path =  here::here(dir_name,'squads',file_wc_teams_data)) %>% 
    mutate(wc_cup_team_texto = purrr::map(team_path,readr::read_lines)) %>% 
    select(-file_wc_data,-team_path) %>% 
    tidyr::separate(dir_name,into = c('year','location'),sep='--')
# wc_teams_by_cup_data %>% tidyr::unnest(wc_cup_team_texto)

# parsear torneo -------------------------------------------------------

# generador de indices que son iguales
# paste0("'",wc_cups_data$year[1:17],"'=c(1,4,5,13)",collapse = ",")
# tail(wc_cups_data$year)
# indices para substring fechas y orden partidos.
# averiguado a mano.de 1930 a 2002 se puede los mismos idx, de ahi en mas cambia, porque se agrega horario
listado_indices_split <- list('1930'=c(1,4,5,13),'1934'=c(1,4,5,13),'1938'=c(1,4,5,13),'1950'=c(1,4,5,13),'1954'=c(1,4,5,13),'1958'=c(1,4,5,13),'1962'=c(1,4,5,13),'1966'=c(1,4,5,13),'1970'=c(1,4,5,13),'1974'=c(1,4,5,13),'1978'=c(1,4,5,13),'1982'=c(1,4,5,13),'1986'=c(1,4,5,13),'1990'=c(1,4,5,13),'1994'=c(1,4,5,13),'1998'=c(1,4,5,13),'2002'=c(1,4,5,13),
                              '2006'=c(1,4,5,19),
                              '2010'=c(1,4,5,23),
                              '2014'=c(1,4,5,23))

wc_cups_data <- wc_cups_data %>% tidyr::separate(dir_name,into = c('year','location'),sep='--')
# wc_cups_data[1,]$wc_cup_texto
wc_cups_data <- wc_cups_data %>% 
    mutate(grupos=purrr::map(.x=wc_cup_texto,.f=parse_copas_extraer_grupos)) %>% 
    mutate(partidos_crudo=purrr::map(.x=wc_cup_texto,.f=extraer_partidos)) %>% 
    mutate(partidos_parsed=purrr::pmap(.l=list(partidos_data=partidos_crudo,
                                              listado_indices=listado_indices_split),
                                       .f=parse_partidos))# %>%
# 
    # NULL


wc_cups_data$partidos_parsed[2]
wc_cups_data$partidos_crudo[2]

# parsear equipos ---------------------------------------------------------
# aca hay que armar un parser de equipos y un parser de copas.

ej_datos_copa1 <- wc_cups_data[1,]$wc_cup_texto
ej_datos_copa2 <- wc_cups_data[20,]$wc_cup_texto


# generador de indices que son iguales
paste0("'",wc_cups_data$year[1:17],"'=c(1,4,5,13)",collapse = ",")
# tail(wc_cups_data$year)





#1 hasta index "blank" +1
#desde index blank+1 +1 +

texto <- wc_cups_data[3,]$wc_cup_texto

stringr::str_starts(string=texto,pattern = "#")

wc_cups_data$partidos_crudo[3]

wc_cups_data %>% select(partidos_crudo) %>% unnest() %>% pull() %>% 
    write_lines(x = .,path=here::here("r","export_partidos_all.txt"))


wc_cups_data$partidos_crudo
current_idx <- 1
partidos_data <- wc_cups_data$partidos_crudo[[current_idx]]
listado_indices <- listado_indices_split[[current_idx]]


resutlado_1 <- parser_copas(ej_datos_copa1)
resutlado_1$
# $partidos %>% View()
parser_copas(ej_datos_copa2)
    

texto <- ej_datos_copa1[[1]]
nombre_copa <- texto[[2]]



grupos_regex <- "Group" %R% one_or_more(rebus::BLANK) %R% one_or_more(rebus::ALNUM)%R% one_or_more(rebus::BLANK)%R% rebus::PIPE
grupos <- texto[texto %>% stringr::str_detect(grupos_regex)]

grs <- data.frame(grupo_linea=c(''))
grs <- grs[-1,]
mchs <- NA # aca datos match + goles

# es grupo
grupos_regex <- "Group" %R% one_or_more(rebus::BLANK) %R% one_or_more(rebus::ALNUM)%R% one_or_more(rebus::BLANK)%R% rebus::PIPE
grupo_match_regex <- "Group" %R% one_or_more(rebus::BLANK) %R% one_or_more(rebus::ALNUM)%R% "\\:"
# es match
# si seccion = "Group 1:"  , es match
# es gol
# ""
# linea_ant <- NA
# for(linea_idx in 1:length(texto)){
#     
#     if ( texto[linea_idx] %>% str_detect(grupos_regex)) {
#         grs <- grs %>% dplyr::add_row(grs)
#     }
#     
#     if ( texto[linea_idx] %>% str_detect(grupo_match_regex)) {
#         grs <- grs %>% dplyr::add_row(grs)
#     }
#     
#     linea_ant <- texto[linea_idx]
# }


# para version JULIO ------------------------------------------------------

# "TORNEO: " > para identificar linea nombre torneo

# "GRUPO: " > para identificar linea GRUPOS
# "FECHA: " > para identificar linea FECHAS PARTIDOS > esta sirve para despues cruzar con los matchs
# "MATCH: " > para identificar linea MATCHS / PARTIDOS >esta ahora incluye los goles
# hay que lograr parsear eso , si funciona traduzco el resto de los mundilaes a mano.
