library(mongolite)

#BANCOS
datosBancos <- read.csv(file = "files/Bancos.csv", sep = ";")
origen <- mongo("balance", db = "repositorio", url = "mongodb://admin:mipassword@localhost:27017/?authSource=admin")

datosBancos$cuenta <- as.character(datosBancos$cuenta)
datosBancos$saldo <- round(as.double(as.character(datosBancos$saldo))*1000, 2)
origen$insert(datosBancos)

#COOPERATIVAS
datosCooperativas <- read.csv(file = "files/CooperativasCodigo.csv", sep = ";")

datosCooperativas$cuenta <- as.character(datosCooperativas$cuenta)
datosCooperativas$saldo <- round(as.double(as.character(datosCooperativas$saldo))*1000, 2)
origen$insert(datosCooperativas)
