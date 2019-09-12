library(lubridate)

almacenaHistorico <- function(historialCollection, fechaActual, fechaConsulta, idOrigen, institucionesNoProcesadas, numReintento, estado, identificador_comun) {
  listaInstituciones <- NA
  for (idx in 1:length(institucionesNoProcesadas)) {
    if (institucionesNoProcesadas[idx] != "Inicio")
      listaInstituciones <- if (is.na(listaInstituciones)) listaInstituciones = institucionesNoProcesadas[idx] else paste(listaInstituciones, ",", institucionesNoProcesadas[idx], sep = "")
  }
  
  historico <- data.frame("identificador_comun" = identificador_comun,
                          "fecha_ejecucion" = fechaActual,
                          "fecha_consulta" = fechaConsulta,
                          "origen" = idOrigen,
                          "instituciones_no_procesadas" = listaInstituciones,
                          "num_reintento" = numReintento,
                          "estado" = estado)

  historialCollection$insert(historico)
  info(logger, "Genera entrada en el historial.")
}

actualizaHistorico <- function(historialCollection, mongoID, estado) {
  q <- paste('{ "_id": { "$oid" : "', mongoID, '" } }', sep = "")
  u <- paste('{ "$set" : { "estado" :"', estado, '"} }', sep = "")
  historialCollection$update(q, u) 
  info(logger, paste("Actualizado entrada historial:", mongoID))
}

# Obtiene la fecha de consulta en la fuente a partir de la fecha actual y la frecuencia
obtenerPeriodo <- function(frecuencia, fechaActual = today()) {
  if (frecuencia == "diaria") {
    fechaConsulta <- fechaActual - days(1)
  } else if (frecuencia == "mensual") {
    fechaConsulta <- rollback(fechaActual)
  } else if (frecuencia == "anual") {
    fechaConsulta <- floor_date(fechaActual, "year") - days(1)
  }
  
  return(fechaConsulta)
}