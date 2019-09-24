library(mongolite)
library(httr)
library(rvest)
library(readxl)

# Lee archivo de propiedades con los parametros de conexion a MongoDB
properties <- read.properties(file = "resources/application.properties")
# Genera una variable URL concatenando los parametros de conexion de MongoDB
conn <- paste("mongodb://", properties$user.mongodb, ":", properties$pass.mongodb, "@", properties$host.mongodb, ":", properties$port.mongodb, "/?authSource=", properties$auth.mongodb, sep = "")
meses <- c("Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic")

ejecutar_proceso <- function(fechaConsulta, configuracion, instituciones) {
  # Obtiene el año y mes a partir de la fecha de consulta
  mes <- meses[month(as.POSIXlt(fechaConsulta))]
  anio <- format(fechaConsulta, "%y")
  dateFile <- paste(mes, anio, sep = "")
  
  # Genera ubicacion de descarga del archivo
  destFile <- paste("files/", configuracion["prefijo_documento"], "_", dateFile, "_", configuracion["segmento"], ".", configuracion["formato"], sep = "")
  
  # Consulta el enlace de descarga del archivo directamente en la página web
  url <- as.character(configuracion["url"])
  webpage <- read_html(url)
  enlaces <- webpage %>% html_nodes('a') %>% html_attr('href')
  
  # Selecciona los enlaces que contienen la palabra Boletin_financiero
  # Selecciona los enlaces que corresponden a la fecha de consulta
  # Selecciona el enlace que corresponde al segmento diferente de 1, pudiendo ser 2, 3, MUT
  boletines <- enlaces[grep("Boletin_financiero", enlaces, ignore.case = TRUE)]
  boletinesAnioActual <- boletines[grep(anio, boletines, ignore.case = TRUE)]
  boletinesMesActual <- boletinesAnioActual[grep(mes, boletinesAnioActual, ignore.case = TRUE)]
  linkSegmento <- boletinesMesActual[grep(configuracion["segmento"], boletinesMesActual, ignore.case = TRUE)]
  
  info(logger, paste("Pagina web del archivo:", url))
  
  # Esta variable permite almacenar en la base de datos si existio o no archivo para descargar y volver a reprocesar 
  institucionesNoProcesadas <- c("Inicio")
  if (length(linkSegmento) > 0) {
    link <- paste(substr(url, 0, 23), linkSegmento[1], sep = "") 
    info(logger, paste("Enlace de descarga del archivo:", link))
    info(logger, paste("Ubicacion de descarga:", destFile))
    
    # Valida si existe el archivo, sino existe lo descarga
    if (!file.exists(destFile))
      download.file(link, destFile, method = "curl")
    print(configuracion["sheet"])
    # Lee del archivo descargado la hoja 3 que corresponde a los balances
    tabla <- as.data.frame(read_excel(destFile, sheet = as.integer(configuracion["sheet"])))
    
    # Realiza un formateo a la hoja eliminado filas y columnas innecesarias
    tabla <- tabla[-c(1:6), ]
    tabla <- tabla[, -c(3:4)]
    tabla <- tabla[,colSums(is.na(tabla)) < nrow(tabla)]
    tabla <- tabla[1:(length(tabla)-1)] # Para el archivo S2, S3 se elimina las ultima columna ya que es total
    tabla <- na.omit(tabla) # Omite las filas con NA
    
    columnaCuenta <- tabla[2:nrow(tabla), 1]
    columnaDescripcion <- tabla[2:nrow(tabla), 2]
    
    balance <- mongo("balance", db = "repositorio", url = conn) # Cambiar a balance en ves de test
    institucionDB <- mongo("institucion_financiera", db = "administracion_sig", url = conn)
    
    for (column in 3:length(tabla)) {
      #TODO: se debe buscar el elemento del archivo y compararlo con el elemento que envia el reproceso o el proceso normal
      institucion <- institucionDB$find(paste('{ "nombre_largo": "', tabla[1, column], '", "estatus": true }', sep = ""))
      if (length(institucion)) {
        info(logger, paste("Institucion:", institucion$nombre_corto))
        
        newTabla <- data.frame("fecha" = fechaConsulta,
                               "institucion" = institucion$codigo,
                               "cuenta" = columnaCuenta,
                               "descripcion" = columnaDescripcion,
                               "saldo" = as.numeric(gsub(",","",tabla[2:nrow(tabla), column])),
                               "origen" = paste("seps_balances_", tolower(configuracion["segmento"]), sep = ""))
        
        balance$insert(newTabla)
        info(logger, paste("Registros almacenados:", nrow(tabla) - 1))
      } else {
          warn(logger, paste("La institucion ", tabla[1, column], ", existe en el archivo pero no en la base de datos."))
          institucionesNoProcesadas <- c(institucionesNoProcesadas, institucion$codigo)  
      }
    }
  } else {
    error(logger, "No existe documento disponible todavía.")
    institucionesNoProcesadas <- c(institucionesNoProcesadas, "todas")
  }
  
  return(institucionesNoProcesadas)
}