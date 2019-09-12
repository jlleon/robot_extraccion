#==============================================================================================================#
#                         Clase que ejecuta el proceso de extraccion de datos
#==============================================================================================================#
# install.packages('mongolite')
# install.packages('properties')
# install.packages('log4r')
# install.packages('lubridate')
# install.packages('rvest')
# install.packages("xlsx")
# install.packages("rjson")

# importo libreria de base de datos MongoDB
library(mongolite)
library(properties)
library(log4r)
library(lubridate)
library(rjson)

source('funciones_genericas.R')
#Crea archivo log
logger <- create.logger(logfile = paste("logs/process_", format(today(), "%Y%m%d"), ".log", sep = ""), level = "INFO")

# Funcion principal
main <- function() {
  # Lee archivo de propiedades con los parametros de conexion a MongoDB
  properties <- read.properties(file = "resources/application.properties")
  # Genera una variable URL concatenando los parametros de conexion de MongoDB
  url <- paste("mongodb://", properties$user.mongodb, ":", properties$pass.mongodb, "@", properties$host.mongodb, ":", properties$port.mongodb, "/?authSource=", properties$auth.mongodb, sep = "")
  
  # Se conecta a la base de datos MongoDB y obtiene la coleccion origen
  origen <- mongo("origen", db = "procesos_batch", url = url)
  historial <- mongo("historial", db = "procesos_batch", url = url)
  
  # Retorna los origenes que tengan estado activo
  listaOrigenes <- origen$find(query = '{ "estatus": true}', fields = '{ "_id": 1 }')

  # Obtiene fecha actual de procesamiento
  # fechaActual = today()
  fechaActual <- ymd('20190201')
  # Esta funcion por cada origen ejecuta el proceso de batch
  f <- function(origenes, salida) {
    # Obtiene el documento de origen, la configuracion del origen y el nombre de proceso
    fuente <- origen$find(gsub('idOrigen', origenes['_id'], '{ "_id": "idOrigen"}'))
    configuracion <- fuente$configuracion
    proceso <- paste("process/", configuracion$nombre_proceso, ".R", sep = "")
    
    info(logger, gsub("nombre_proceso", configuracion$nombre_proceso, "=== Ejecutando origen: nombre_proceso ==="))
    info(logger, gsub("fecha_actual", fechaActual, "Fecha Actual: fecha_actual"))
    
    # Obtiene la fecha consulta y obtiene el historial de ejecucion
    fechaConsulta <- obtenerPeriodo(configuracion$frecuencia, fechaActual)
    # fechaConsulta <- ymd("20190430")
    info(logger, gsub("fecha_consulta", fechaConsulta, "Fecha Consulta: fecha_consulta"))
    
    queryHistorial <- '{ "fecha_consulta": "fechaConsulta", "origen": "idOrigen" }'
    queryHistorial <- gsub('fechaConsulta', fechaConsulta, queryHistorial)
    queryHistorial <- gsub('idOrigen', origenes['_id'], queryHistorial)
    historialDF <- as.data.frame(historial$find(queryHistorial))
    
    # Verifica historial (Si no existe un antecedente de ejecucion)
    if (nrow(historialDF) == 0) { # No existe historial
      # Obtengo listado de instituciones financieras
      instituciones <- mongo("institucion_financiera", db = "administracion_sig", url = url)
      listadoInstituciones <- instituciones$find(gsub("idOrigen", origenes['_id'], '{ "origen": "idOrigen", "estatus": true }'))
      
      # Llama al proceso
      if (file.exists(proceso)) {
        source(proceso)
        institucionesNoProcesadas <- ejecutar_proceso(fechaConsulta, configuracion, listadoInstituciones)
        estado <- if(length(institucionesNoProcesadas) > 1) 'reintentar' else 'finalizado_correcto'
        identificador_comun <- paste(origenes['_id'], "_", fechaConsulta, sep = '')
        almacenaHistorico(historial, fechaActual, fechaConsulta, origenes['_id'], institucionesNoProcesadas, 0, estado, identificador_comun)
      }
    } else {
      warn(logger, gsub("fecha_consulta", fechaConsulta, "Ya se ejecuto proceso para el periodo actual: fecha_consulta"))
    }
  }
  
  # Similar a realizar un for por cada fila del dataframe listaOrigenes ejecuta la funcion f
  apply(listaOrigenes, 1, f)
}

# Valida que no existan errores en el proceso y en caso de que falle vuelva a ejecutar
tryCatch(
  expr = {
    info(logger, "***** Inicia proceso de consulta de datos en origenes *****")
    main()
  }, error = function(e) {
    error(logger, e)
  }, warning = function(w) {
    warn(logger, w)
  }, finally = {
    info(logger, "***** Finaliza proceso de consulta de datos en origenes *****")
  }
)

