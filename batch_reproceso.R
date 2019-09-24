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
library(rvest)
library(readxl)

source('funciones_genericas.R')
#Crea archivo log
logger <- create.logger(logfile = paste("logs/reprocess_", format(today(), "%Y%m%d"), ".log", sep = ""), level = "INFO")

# Funcion principal
main <- function() {
  # Lee archivo de propiedades con los parametros de conexion a MongoDB
  properties <- read.properties(file = "resources/application.properties")
  # Genera una variable URL concatenando los parametros de conexion de MongoDB
  url <- paste("mongodb://", properties$user.mongodb, ":", properties$pass.mongodb, "@", properties$host.mongodb, ":", properties$port.mongodb, "/?authSource=", properties$auth.mongodb, sep = "")
  
  # Se conecta a la base de datos MongoDB y obtiene la coleccion origen
  origen <- mongo("origen", db = "procesos_batch", url = url)
  historial <- mongo("historial", db = "procesos_batch", url = url)
  instituciones <- mongo("institucion_financiera", db = "administracion_sig", url = url)
  fechaActual <- today()
  
  # Se obtiene los procesos a reintentar
  reintentos <- historial$find(query = '{ "estado": "reintentar" }', fields = '{ "_id": 1, "identificador_comun": 1, "fecha_consulta": 1, "origen": 1, "num_reintento": 1, "estado": 1, "instituciones_no_procesadas": 1}')
  if (nrow(reintentos) > 0) {
    for (row in 1:nrow(reintentos)) {
      # Obtiene fila de reintentos
      reintento <- reintentos[row,]
      
      # Obtiene los campos de la fila de reintentos
      fechaConsulta <- ymd(reintento$fecha_consulta)
      identificador_comun <- reintento$identificador_comun
      numeroReintento <- reintento$num_reintento
      institucionesNoProcesadas <- strsplit(reintento$instituciones_no_procesadas, "[,]")
      
      # Obtiene datos del origen
      fuente <- origen$find(gsub('id_reintento', reintento$origen, '{ "_id": "id_reintento" }'))
      configuracion <- fuente$configuracion
      proceso <- paste("process/", configuracion$nombre_proceso, ".R", sep = "")
      
      # Valida los dias de reintentos
      parametroReintento <- configuracion["num_reintentos"]
      if (numeroReintento < parametroReintento) {
        info(logger, paste("Reprocesando reintento con fecha_consulta:", fechaConsulta, ". Origen:", reintento$origen))
        info(logger, paste("El proceso es:", proceso))
        
        # Genera query para consultar las instituciones a reprocesar
        # "todas" significa que va a procesar todas las instituciones, 
        # caso contrario obtiene el listado de las instituciones que no pudo procesa normalmente 
        query <- NA
        if (institucionesNoProcesadas[1] != 'todas') {
          for (idx in 1:length(institucionesNoProcesadas)) {
            query <- if (is.na(query)) paste('{"codigo": ', institucionesNoProcesadas[idx], '}', sep = "") 
            else paste(query, ", ", ' {"codigo": ', institucionesNoProcesadas[idx], '}', sep = "" )
          }  
          query <- gsub("condiciones", query, '{ "$or": [ condiciones ]}')
        } else {
          query <- gsub("idOrigen", reintento$origen, '{ "origen": "idOrigen"}')
        }
        
        # Obtiene los datos de las instituciones
        listadoInstituciones <- instituciones$find(query)
        source(proceso)
        institucionesNoProcesadas <- ejecutar_proceso(fechaConsulta, configuracion, listadoInstituciones)
        estado <- if(length(institucionesNoProcesadas) > 1) 'reintentar' else 'finalizado_correcto'
        
        # actualiza historial actual
        actualizaHistorico(historial, reintento["_id"], "reprocesado")
        almacenaHistorico(historial, fechaActual, fechaConsulta, reintento$origen, institucionesNoProcesadas, numeroReintento+1, estado, identificador_comun)
      } else {
        warn(logger, paste("No se pudo reprocesar correctamente. Se completaron el numero de reintentos:", numeroReintento))
        actualizaHistorico(historial, reintento["_id"], "finalizado_incorrecto")
      }
    }  
  }
}

# Valida que no existan errores en el proceso y en caso de que falle vuelva a ejecutar
tryCatch(
  expr = {
    info(logger, "***** Inicia reproceso de datos con estado de reintento *****")
    main()
  }, error = function(e) {
    error(logger, e)
  }, warning = function(w) {
    warn(logger, w)
  }, finally = {
    info(logger, "***** Finaliza reproceso de datos *****")
  }
)