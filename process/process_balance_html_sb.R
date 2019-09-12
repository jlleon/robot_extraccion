# =================================================================================================
#             PROCESO DE EXTRACCION DE DATOS DE UNA PAGINA HTML DE LA PAGINA DE LA SB
# =================================================================================================

library(lubridate)
library(rvest)
# Lee archivo de propiedades con los parametros de conexion a MongoDB
properties <- read.properties(file = "resources/application.properties")
# Genera una variable URL concatenando los parametros de conexion de MongoDB
conn <- paste("mongodb://", properties$user.mongodb, ":", properties$pass.mongodb, "@", properties$host.mongodb, ":", properties$port.mongodb, "/?authSource=", properties$auth.mongodb, sep = "")

# Funcion principal
ejecutar_proceso <- function(fechaConsulta, configuracion, instituciones) {
  # A partir de la fechaConsulta se toma el año y el mes
  anio <- format(fechaConsulta, "%Y")
  mes <- format(fechaConsulta, "%m")
  
  # Define lista de instituciones no procesadas
  # Obtiene los datos por cada institucion
  institucionesNoProcesadas <- c("Inicio")
  balance <- mongo("balance", db = "repositorio", url = conn)
  for (row in 1:nrow(instituciones)) {
    # Define la URL de donde va a leer
    url <- configuracion["url"]
    parameters <- names(configuracion)
    for (parameter in parameters){
      if (parameter != "url" && parameter != "nombre_proceso" && parameter != "frecuencia" && parameter != "num_reintentos")
        url <- gsub(paste("!", parameter, sep = ""), configuracion[parameter], url)
    }
    
    # Setea los parametros de anio, mes y codigo de institucion
    url <- gsub("!anio", anio, url)
    url <- gsub("!mes", mes, url)
    url <- gsub("!cod_institucion", instituciones[row, "codigo"], url)
    
    # Lee de la página web
    info(logger, paste("Institucion:", instituciones[row, "nombre_corto"]))
    info(logger, paste("URL Consulta:", url))
    webpage <- read_html(url)
    datos <- webpage %>% html_nodes('table')
    lista <- datos[[3]] %>% html_table()
    tabla <- as.data.frame(lista)
    
    if (nrow(tabla) > 1) {
      tabla$institucion <- instituciones[row, "codigo"]
      tabla$origen <- 'sb_balances'
      tabla$fecha <- fechaConsulta
      colnames(tabla)[1] <- "cuenta"
      colnames(tabla)[2] <- "descripcion"
      colnames(tabla)[3] <- "saldo" 
      
      tabla <- tabla[-c(1:4), ]
      tabla[,3] <- as.numeric(gsub(',', '', tabla[,3]))
      tabla <- tabla[,c(6,4,1,2,3,5)]
      
      balance$insert(tabla)
      info(logger, paste("Registros Almacenados:", nrow(tabla)))
    } else {
      warn(logger, tabla$X1)
      institucionesNoProcesadas <- c(institucionesNoProcesadas, instituciones[row, "codigo"])
    }
  }
  
  return(institucionesNoProcesadas)
}