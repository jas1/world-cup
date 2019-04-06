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
        tidyr::separate(paises,into=c("pais1","pais2","pais3","pais4","pais5","pais6","pais7","pais8","pais9","pais10")) %>% 
        tidyr::gather(gr,pais,pais1,pais2,pais3,pais4) %>% 
        filter(!is.na(pais)) %>% 
        select(-gr,-grupo) %>% 
        rename(grupo=name)
    grupos_df
}

extraer_finales <- function(datos_copa){
    retu <- NA
    if (length(datos_copa) > 1) {
        retu <-extraer_partidos(datos_copa)    
    }
    retu
}

extraer_partidos <-  function(datos_copa){
    texto <- datos_copa
    
    no_comentarios <- texto[stringr::str_starts(string=texto,pattern = "#",negate = TRUE)]
    partidos <- no_comentarios[stringr::str_detect(string = no_comentarios,pattern = "@")] %>% stringr::str_trim()
    
    partidos
}
parse_finales <- function(partidos_data,listado_indices,listado_fechas,anio){
    retu <- NA
    if (length(partidos_data) > 1) {
        retu <-parse_partidos(partidos_data,listado_indices,listado_fechas,anio)    
    }
    retu
}
parse_partidos <- function(partidos_data,listado_indices,listado_fechas,anio){
    
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
    partidos_full_crudo <- data.frame(partidos=partidos_data,stringsAsFactors = FALSE) %>% as_tibble() 
    partidos_estadio_ciudad <- partidos_full_crudo %>% 
        tidyr::separate(partidos,sep="@",into=c("partido","estadio")) %>% 
        tidyr::separate(estadio,sep=",",into=c("estadio","ciudad")) %>% 
        mutate(estadio=str_trim(estadio)) %>% 
        mutate(ciudad=str_trim(ciudad)) 
    
    partidos_pos_fecha <- partidos_estadio_ciudad %>% 
        mutate(partido_orden=stringr::str_trim(stringr::str_sub(partido,listado_indices[1],listado_indices[2])))%>% 
        mutate(fecha=stringr::str_trim(stringr::str_sub(partido,listado_indices[3],listado_indices[4]))) %>% 
        mutate(fecha=paste0(fecha," ",anio)) %>% # YmdHMS
        mutate(fecha=parse_date(fecha,format=listado_fechas))
 
    # df <- data.frame(textos=c(ex1,ex2,ex3,ex4,ex5,ex6,ex7),stringsAsFactors = FALSE) %>% as_tibble()
    # ex1<- "(55) Tue Jun/29 16:00   Paraguay       0-0 0-0nV 5-3iE  Japan         @ Loftus Versfeld Stadium, Pretoria"
    # ex2<- "(58) Fri Jul/2 20:30   Uruguay        1-1 1-1nV 4-2iE   Ghana         @ Soccer City, Johannesburg"
    # ex3<- "(64) Sun Jul/11 20:30  Netherlands  0-0 0-1nV  Spain     @ Soccer City, Johannesburg"
    # ex4<- "(64) Sun Jul/13 16:00  Germany 1-0 a.e.t. (0-0, 0-0) Argentina     @ Estádio do Maracanã, Rio de Janeiro (UTC-3)   # W61 - W62"
    # ex5<- "(62) Wed Jul/9 17:00  Netherlands  2-4 pen. 0-0 a.e.t. (0-0, 0-0)  Argentina        @ Arena de São Paulo, São Paulo (UTC-3)    # W59 - W69  "
    # ex6<- "(45) 21 June    Brazil  3-4pen 1-1aet 1-1  France          @ Estadio Jalisco, Guadalajara"
    # ex7<- "(38) 15 June   Soviet Union  3-4 a.e.t.  2-2   Belgium  @ Estadio Nou Camp, León"
    # 
    # 
    # df %>% 
    #     tidyr::separate(textos,into=c("partido","estadio"),sep="@") %>% 
    #     mutate(equipos=stringr::str_trim(stringr::str_sub(partido,start = 13))) %>% #13 o 22
    #     mutate(equipos= stringr::str_replace_all(equipos,"iE","")) %>% 
    #     mutate(equipos= stringr::str_replace_all(equipos,"nV","")) %>% 
    #     # mutate(equipos= stringr::str_replace_all(equipos,"aet","")) %>% 
    #     mutate(equipos= stringr::str_replace_all(equipos,"a"%R%rebus::optional(rebus::DOT)%R%"e"%R%rebus::optional(rebus::DOT)%R%"t"%R%rebus::optional(rebus::DOT),"")) %>% 
    #     mutate(equipos= stringr::str_replace_all(equipos,"pen"%R%rebus::optional(rebus::DOT),"")) %>% 
    #     mutate(equipos=str_replace_all(equipos,"[^[:alpha:][:space:]]","")) %>% 
    #     
    #     mutate(equipos=stringr::str_extract_all(equipos,regex_equipos_fechas)) %>% 
    #     mutate(equipos=purrr::map(equipos,.f=function(x){paste0(collapse=";",x)})) %>% tidyr::unnest(equipos) %>% #View()
    #     tidyr::separate(equipos,sep=";",into=c("nombre_eq_1","nombre_eq_2")) %>% 
    #     mutate(nombre_eq_1=stringr::str_trim(nombre_eq_1)) %>% #pull(equipos)
    #     mutate(nombre_eq_2=stringr::str_trim(nombre_eq_2)) %>% 
    #     select(nombre_eq_1,nombre_eq_2)
        # mutate(equipos= stringr::str_replace_all(equipos,"pen","")) %>% 

    
    # si contiene iE , si contiene nV, es de un tipo de partidos si tiene iE, es sacar 
    # " 5-3iE" creo que es penales
    # " 0-1nV" creo que es entre tiempo / gol de oro
    # si contiene a.e.t., es otro, : " a entretiempo / gol de oro " , 
    #
    # si contiene  pen. es otro: penales
    # 
    
    
    partidos_nombre_equipos <- partidos_pos_fecha %>% 
        mutate(equipos=stringr::str_trim(stringr::str_sub(partido,start = listado_indices[4]+1))) %>%
        mutate(equipos= stringr::str_replace_all(equipos,"iE","")) %>% 
        mutate(equipos= stringr::str_replace_all(equipos,"nV","")) %>% 
        # mutate(equipos= stringr::str_replace_all(equipos,"aet","")) %>% 
        mutate(equipos= stringr::str_replace_all(equipos,"a"%R%rebus::optional(rebus::DOT)%R%"e"%R%rebus::optional(rebus::DOT)%R%"t"%R%rebus::optional(rebus::DOT),"")) %>% 
        mutate(equipos= stringr::str_replace_all(equipos,"pen"%R%rebus::optional(rebus::DOT),"")) %>% 
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
        tidyr::separate(resultado,sep=";",into=c("goles_eq_1","goles_eq_2")) 
 
    partidos_filtro_final<- partidos_resultados %>% 
        select(-partido,partido_orden,fecha,nombre_eq_1,nombre_eq_2,goles_eq_1,goles_eq_2)
    
    partidos_filtro_final
}
# Armar origenes de datos ------------------------------------------------------

wc_path_dirs_regex <- rebus::DGT %R% rebus::DGT %R% rebus::DGT %R% rebus::DGT %R% "--"
wc_data <- list.files(path = here::here(),pattern = wc_path_dirs_regex) %>% 
    tibble::enframe() %>% 
    rename(dir_name = value) %>% 
    mutate(dir_name_2 = dir_name) %>% 
    tidyr::separate(dir_name_2,into = c('anio','sponsor'),sep='--') %>% 
    mutate(file_wc_data= purrr::map(dir_name,.f = function(x){
        file_name <- "cup.txt"
        read_cup_data <- here::here(x,file_name)
        retu <- file.exists(read_cup_data)
        if (!retu) {
            retu <- NA
        }else{
            retu <- file_name
        }
        retu
    }) ) %>% 
    tidyr::unnest(file_wc_data) %>%
    mutate(file_wc_finals_data= purrr::map(dir_name,.f = function(x){
        file_name <- "cup_finals.txt"
        read_cup_data <- here::here(x,file_name)
        retu <- file.exists(read_cup_data)
        if (!retu) {
            retu <- NA
        }else{
            retu <- file_name
        }
        retu
    }) ) %>% 
    tidyr::unnest(file_wc_finals_data) %>% 
    mutate(file_wc_teams_data = purrr::map(dir_name,.f = function(x){
        read_cup_data <- here::here(x,"squads")
        retu <- list.files(read_cup_data)
        if (length(retu) == 0) {
            retu <- NA
        }
        retu
    }) ) %>% 
    tidyr::unnest(file_wc_teams_data) %>% 
    
    filter(anio<lubridate::year(lubridate::today())) # para filtrar a los futuros / mismo año

# wc_data %>% count(dir_name) %>% View()

# obtener los datos de los archivos ------------------------------------------------------

wc_cups_data <- wc_data %>% 
    nest(-anio,-sponsor,-dir_name,-file_wc_data,-file_wc_finals_data,.key = archivos_equipos) %>% # no funciona porque no tengo todos los equipos aca.
    mutate(current_path =  here::here(dir_name,file_wc_data)) %>% 
    mutate(wc_cup_texto = purrr::map(current_path,readr::read_lines)) %>% 
    mutate(wc_cup_finals_texto = purrr::map( .x=here::here(dir_name,file_wc_finals_data),
                                             .f=function(x){
                                                 
                                                 retu <- NA
                                                 if (!stringr::str_ends(x,"NA")) {
                                                     retu <- readr::read_lines(x)   
                                                 }
                                                 retu
        
    })) %>% 
    select(-file_wc_data,-current_path)
View(wc_cups_data)

# parsear torneo -------------------------------------------------------

# generador de indices que son iguales
# paste0("'",wc_cups_data$year[1:17],"'=c(1,4,5,13)",collapse = ",")
# tail(wc_cups_data$year)
# indices para substring fechas y orden partidos.
# averiguado a mano.de 1930 a 2002 se puede los mismos idx, de ahi en mas cambia, porque se agrega horario
listado_indices_split <- list('1930'=c(1,4,5,13),'1934'=c(1,4,5,13),'1938'=c(1,4,5,13),'1950'=c(1,4,5,13),'1954'=c(1,4,5,13),'1958'=c(1,4,5,13),'1962'=c(1,4,5,13),'1966'=c(1,4,5,13),'1970'=c(1,4,5,13),'1974'=c(1,4,5,13),'1978'=c(1,4,5,13),'1982'=c(1,4,5,13),'1986'=c(1,4,5,13),'1990'=c(1,4,5,13),'1994'=c(1,4,5,13),'1998'=c(1,4,5,13),'2002'=c(1,4,5,13),
                              '2006'=c(1,4,5,19),
                              '2010'=c(1,4,5,23),
                              '2014'=c(1,4,5,23),
                              '2018'=c(1,4,5,23))

listado_indices_split_finales <- list('1930'=c(1,4,5,13),'1934'=c(1,4,5,13),'1938'=c(1,4,5,13),'1950'=c(1,4,5,13),'1954'=c(1,4,5,13),'1958'=c(1,4,5,13),'1962'=c(1,4,5,13),'1966'=c(1,4,5,13),'1970'=c(1,4,5,13),'1974'=c(1,4,5,13),'1978'=c(1,4,5,13),'1982'=c(1,4,5,13),'1986'=c(1,4,5,13),'1990'=c(1,4,5,13),'1994'=c(1,4,5,13),'1998'=c(1,4,5,13),'2002'=c(1,4,5,13),
                              '2006'=c(1,4,5,19),
                              '2010'=c(1,4,5,22),
                              '2014'=c(1,4,5,22),
                              '2018'=c(1,4,5,22))

listado_parse_fecha <- list('1930'='%d %B %Y','1934'='%d %B %Y','1938'='%d %B %Y','1950'='%d %B %Y','1954'='%d %B %Y','1958'='%d %B %Y','1962'='%d %B %Y','1966'='%d %B %Y','1970'='%d %B %Y','1974'='%d %B %Y','1978'='%d %B %Y','1982'='%d %B %Y','1986'='%d %B %Y','1990'='%d %B %Y','1994'='%d %B %Y','1998'='%d %B %Y','2002'='%d %B %Y',
                              '2006'="%a %b/%d %Y",#Wed Jun/21 2006
                              '2010'="%a %b/%d %H:%M %Y",#Thu Jun/24 16:00 2010
                              '2014'="%a %b/%d %H:%M %Y",
                              '2018'="%a %b/%d %H:%M %Y")#Thu Jun/12 17:00 2014
# paste0("'",wc_cups_data$anio[1:17],"'='%d %B %Y'",collapse = ",")
# wc_cups_data[1,]$wc_cup_texto
wc_cups_data_processed_1 <- wc_cups_data[1,] %>% 
    mutate(grupos=purrr::map(.x=wc_cup_texto,.f=parse_copas_extraer_grupos)) %>% 
    mutate(partidos_crudo=purrr::map(.x=wc_cup_texto,.f=extraer_partidos)) %>% 
    mutate(partidos_parsed=purrr::pmap(.l=list(partidos_data=partidos_crudo,
                                              listado_indices=listado_indices_split[1],
                                              listado_fechas=listado_parse_fecha,
                                              anio=anio),
                                       .f=parse_partidos))

wc_cups_data_processed <- wc_cups_data %>% 
    mutate(grupos=purrr::map(.x=wc_cup_texto,.f=parse_copas_extraer_grupos)) %>% 
    mutate(partidos_crudo=purrr::map(.x=wc_cup_texto,.f=extraer_partidos)) %>% 
    mutate(partidos_parsed=purrr::pmap(.l=list(partidos_data=partidos_crudo,
                                               listado_indices=listado_indices_split,
                                               listado_fechas=listado_parse_fecha,
                                               anio=anio),
                                       .f=parse_partidos))


current_idx <- 21
wc_cups_data_processed$partidos_parsed[current_idx]
# wc_cups_data_processed$partidos_parsed[current_idx][[1]] %>% View()
wc_cups_data_processed$partidos_crudo[current_idx]


# creo que con tener ya todos los equipos resultados  es mas que suficiente para esta 1ra edicion
# que esten las fechas bien, traducir y listo
# me falta hacer el merge de los que tienen " final" , a que este todo en un " partidos " y exportar eso.
# si todo va bien, podria reciclar lo que hice con cup.txt

wc_cups_data_processed_con_finales <- wc_cups_data_processed %>% 
    mutate(finales_crudo=purrr::map(.x=wc_cup_finals_texto,.f=extraer_finales)) %>% # select(finales_crudo) %>% View()
    mutate(finales_parsed=purrr::pmap(.l=list(partidos_data=finales_crudo,
                                           listado_indices=listado_indices_split_finales,
                                           listado_fechas=listado_parse_fecha,
                                           anio=anio),
                                   .f=parse_finales))
    

current_idx_2 <- 21
wc_cups_data_processed_con_finales$finales_parsed[current_idx_2]
# wc_cups_data_processed$partidos_parsed[current_idx][[1]] %>% View()
wc_cups_data_processed_con_finales$finales_crudo[current_idx_2]

# wc_cups_data_processed_con_finales %>% filter(!is.na(file_wc_finals_data)) %>% unnest(finales_parsed) %>% 
#     # count(anio,fecha) %>% View()
#     filter(is.na(fecha))

wc_cups_data_processed_con_finales %>% filter(!is.na(file_wc_finals_data)) %>% unnest(finales_parsed) %>% View()

glimpse(wc_cups_data_processed_con_finales)

# parsear equipos ---------------------------------------------------------
wc_teams_by_cup_data <- wc_data %>% 
    mutate(team_path =  here::here(dir_name,'squads',file_wc_teams_data)) %>% 
    # mutate(wc_cup_team_texto = purrr::map(team_path,readr::read_lines)) %>% 
    mutate(wc_cup_team_texto = purrr::map( .x=team_path,
                                           .f=function(x){
                                               retu <- NA
                                               if (!stringr::str_ends(x,"NA")) {
                                                   retu <- readr::read_lines(x)   
                                               }
                                               retu
                                           })) %>% 
    select(-file_wc_data,-team_path)