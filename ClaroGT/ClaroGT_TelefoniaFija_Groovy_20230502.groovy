import groovy.io.FileType
import groovy.time.TimeCategory
import groovy.transform.Synchronized
import groovy.xml.MarkupBuilder
import groovyx.gpars.GParsPool
import groovyx.gpars.actor.Actor
import groovyx.gpars.actor.DefaultActor

import java.math.RoundingMode
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.Paths
import java.security.MessageDigest
import java.text.NumberFormat
import java.time.LocalDate
import java.time.LocalTime
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors
import java.util.zip.CRC32
import java.util.zip.Checksum

class TxtSpecs {

    enum CategoriaType {
        DESCONOCIDO,
        CORPORATIVO,
        MASIVO

        static CategoriaType forCiclo(Integer ciclo) {
            switch (ciclo) {
                case 2:
                case 55:
                case 57:
                case 58:
                    return CORPORATIVO
                default:
                    return MASIVO
            }
        }
    }

    /**
    0900100  LLAMADAS LOCALES (LLAMADA)
    1100100  LLAMADAS A OPERADORES (LLAMADA)
    1100100  LLAMADAS NACIONALES (LLAMADA)
    1100100  OTROS CARGOS (LLAMADA)
    1201100  OTROS CARGOS CLARO TV-PPV (EVENTO)
    1300100  OTROS SERVICIOS (OTRO)
     */
    enum ConsumoType {
        DESCONOCIDO,
        LLAMADA,
        EVENTO,
        OTRO,
    }

    static int LINE_ID_LENGTH = 7

    enum SectionType {
        _0000000('0000000'), // DESCONOCIDO
        _0100000('0100000'), // INICIO
        _0200000('0200000'), // ESTADO DE CUENTA
        _0400200('0400200'), // RESUMEN***
        _0700100('0700100'), // Detalles de llamadas
        _1400100('1400100'), // RESUMEN DE INTEGRADOS
        _1401100('1401100'), // DETALLE DE CARGOS
        _1800100('1800100'), // DETALLE DE ENLACES
        _1600100('1600100'), // DETALLE DE FINANCIAMIENTOS
        _3000000('3000000'), // NOTAS DE ABONADO
        _3200000('3200000'), // NOTAS DE CREDITO
        _5000100('5000100') // FIN

        String id

        SectionType(String id) {
            this.id = id
        }

        static SectionType forId(String id) {
            for(SectionType type : values()) {
                if (type.id == id) {
                    return type;
                }
            }
            return _0000000;
        }

        static boolean startsNewSectionExcludingVeryFirstAndLastSections(String id) {
            SectionType type = forId(id)
            return type != _0000000 && type != _0100000 && type != _5000100
        }

        static boolean endOfMultilineSection(String id) {
            return id == _5000100.id
        }

        static boolean endOfMultilineSection(SectionType sectionType) {
            return sectionType == _5000100
        }
    }

    private static final  SECTIONS = [
        '0100000' : [ // INICIO
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^0100000/:
                                sectionRecord.id = id
                                sectionRecord.segmentacion = line[7..-1].tokenize()[-1]
                                break
                            case ~/^0100100/:
                                def lineV2 = line[82..-1]
                                if (lineV2.length() == 15) {
                                    lineV2 += ' ' * (29 - 15)
                                }
                                sectionRecord.personId = lineV2[-9..-1].trim()
                                sectionRecord.fechaEmision = lineV2[0..<11]
                                sectionRecord.imprimeCiclo = lineV2[11..<12]
                                sectionRecord.conFinanciamiento = lineV2[12..<13]
                                sectionRecord.tipoFactura = lineV2[13..<14]
                                sectionRecord.imprimeBPS = lineV2[14..<15]
                                sectionRecord.refactura = 'N' // Until proven otherwise
                                break
                            case ~/^0100200/:
                                sectionRecord.emisorNombre = line[7..-1].trim()
                                break
                            case ~/^0100300/:
                                sectionRecord.emisorDirL1 = line[7..-1].trim()
                                break
                            case ~/^0100400/:
                                sectionRecord.emisorDirL2 = line[7..-1].trim()
                                break
                            case ~/^0100500/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.emisorDirL3 = tokens[0]
                                sectionRecord.emisorNIT = tokens[2]
                                break
                            case ~/^0100600/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.facturaSerie = tokens[2]
                                if (sectionRecord.facturaSerie == 'NUMERO') {
                                    sectionRecord.facturaSerie = ' '
                                }
                                sectionRecord.numeroAutorizacion = tokens[-1]
                                if (sectionRecord.numeroAutorizacion == 'AUTORIZACION:') {
                                    sectionRecord.numeroAutorizacion = ' '
                                }
                                break
                            case ~/^0100700/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.documentoNumero = tokens[1]
                                if (sectionRecord.documentoNumero == 'PARA') {
                                    sectionRecord.documentoNumero = ' '
                                    sectionRecord.documentoNumeroV2 = sectionRecord.documentoNumero.replace(',', '')
                                    sectionRecord.consultas = tokens[1..-1].join(' ')
                                } else {
                                    sectionRecord.documentoNumeroV2 = sectionRecord.documentoNumero.replace(',', '')
                                    sectionRecord.consultas = tokens[2..-1].join(' ')
                                }
                                break
                            case ~/^0100800/:
                                def tokens = line[7..-1].tokenize()
                                def refTokens = tokens[1].tokenize('//')
//                                sectionRecord.ciclo = refTokens[0]
//                                sectionRecord.correlativo = refTokens[1]
//                                sectionRecord.rutaCourier = tokens[2]
                                sectionRecord.ciclo = refTokens[0]
                                sectionRecord.correlativo = refTokens[1]
                                if (tokens.size() == 8) {
                                    sectionRecord.rutaCourier = tokens[2]
                                } else {
                                    sectionRecord.rutaCourier = ' '
                                }
                                break
                            case ~/^0100900/:
                                sectionRecord.receptorNombre = line[7..-1]
                                break
                            case ~/^0101000/:
                                //sectionRecord.receptorDirL1 = line[8..53].trim()
								sectionRecord.receptorDirL1 = line[7..53].trim()
                                //sectionRecord.receptorTelefono = line[55..63].trim()
                                sectionRecord.receptorTelefono = line[54..63].trim()
                                sectionRecord.totalPagar = {
                                    String data = line[64..84].trim()
                                    if (data.startsWith('Q-')) {
                                        return "-${data.tokenize()[1]}".toString()
                                    } else {
                                        return "${data.tokenize()[1]}".toString()
                                    }
                                }.call()
                                sectionRecord.fechaVencimiento = {
								/*
                                    if (line.size() >= 95) {
                                        return line[84..95].trim()
                                    } else {
                                        return  '01/ENE/1970'
                                    }
								*/
									if (line.size() == 96) {
										return line[85..95].trim()
									} else if (line.size() >= 95) {
										return line[85..96].trim()
									} else {
										return '01/ENE/1970'
									}
                                }.call()
                                sectionRecord.ajuste = {
                                    if (line.size() >= 120) {
                                        String data = line[102..-1].trim()
                                        if (data.startsWith('Q-')) {
                                            return "-${data.tokenize()[1]}".toString()
                                        } else {
                                            return data.tokenize()[1]
                                        }
                                    } else {
                                        sectionRecord.ajuste = '0.00'
										//return '0.00'
                                    }
                                }.call()
                                break
                            case ~/^0101100/: // [Optional]
                                sectionRecord.receptorDirL2 = line[7..-1]
                                break
                            case ~/^0101200/:
                                sectionRecord.receptorDirL3 = line[7..45].trim()
                                //sectionRecord.receptorNIT = line[51..65].trim()
                                //sectionRecord.servicio = line[66..-1].trim()
								// 20230117 - Jasiel
								sectionRecord.receptorNIT = line[51..71].trim()
                                sectionRecord.servicio = line[73..-1].trim()
                                break
                            case ~/^0101300/:
                                sectionRecord.limite = line[7..-1].trim()
                                break
                            case ~/^0101500/:
                                sectionRecord.mensaje = line[7..-1].trim()
                                break
                            case ~/^0101600/:
                                sectionRecord.mensaje += line[34..-1].trim()
                                break
                            case ~/^0101700/:
                                sectionRecord.eMails = line[7..-1].trim()
                                break
                        }
                } catch(Exception e) {
                    Globals.LINE_ERROR = line
                    throw e
                }
             }
                return sectionRecord
            }
        ],
        '0200000' : [ // ESTADO DE CUENTA
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.conceptosCobro = [] // new List
                sectionRecord.financiamientos = [] // new List
                sectionRecord.productosServicios = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^0200000/:
                                sectionRecord.id = id
                                break
                            case ~/^0200100/:
                                // 0200100POR VENTA DE SERVICIO TELEFONICO AL:  10/FEB/2021
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.periodoFacturado = tokens[-1]
                                break
                            case ~/^0200200/:
                                // 0200200RESUMEN CONCEPTO DE COBRO
                                //sectionRecord.conceptosCobro = [] // new List
                                break
                            case ~/^0200300/: // SALDO ANTERIOR
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.periodo = tokens.pop()
                                conceptoCobroSectionRecord.valor = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                conceptoCobroSectionRecord.nombre = tokens.join(' ')
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0200400/: // SU PAGO GRACIAS
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.valor = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                conceptoCobroSectionRecord.nombre = 'PAGOS EFECTUADOS (-)'
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0200500/: // SALDO INICIAL
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.periodo = tokens.pop()
                                conceptoCobroSectionRecord.valor = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                conceptoCobroSectionRecord.nombre = tokens.join(' ')
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0200600/: // CARGOS DEL MES
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.nombre = tokens.join(' ')
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0200700/: // FINANCIAMIENTO
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.valor = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                conceptoCobroSectionRecord.nombre = tokens.join(' ')
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0200800/: // SALDO INICIAL + FINANCIAMIENTO
                                def tokens = line[7..-1].tokenize()
                                TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
                                conceptoCobroSectionRecord.valor = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                conceptoCobroSectionRecord.nombre = tokens.join(' ')
                                sectionRecord.conceptosCobro << conceptoCobroSectionRecord
                                break
                            case ~/^0300000/: // FINANCIAMIENTOS
                                //sectionRecord.financiamientos = []
                                break
                            case ~/^0300010/:
                                def tokens = line[7..-1].tokenize()
                                if (tokens.size() > 1) { // != TOTAL
                                    TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
                                    cargoMesSectionRecord.valor = {
                                        def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                        def currency = tokens[-1]
                                        if (currency == 'Q-') {
                                            valor = "-${valor}"
                                        }
                                        return valor
                                    }.call()
                                    if (tokens[-1] == 'Q' || tokens[-1] == 'Q-') {
                                        tokens.pop() // Remoe last token
                                    }
                                    cargoMesSectionRecord.nombre = tokens.join(' ')
                                    sectionRecord.financiamientos << cargoMesSectionRecord
                                } else { // == TOTAL
                                    TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
                                    cargoMesSectionRecord.nombre = tokens.join(' ')
                                    sectionRecord.financiamientos << cargoMesSectionRecord
                                }
                                break
                            case ~/^0300100/: // PRODUCTOS Y SERVICIOS
                                sectionRecord.productosServicios = []
                                break
                            case ~/^0300110/:
                                def tokens = line[7..-1].tokenize()
                                def text = []
                                // TODO: if 'VER DETALLE' changes it won't work
                                if (line.endsWith('VER DETALLE')) {
                                    text << tokens.pop() // remove last element
                                    text << tokens.pop() // remove last element
                                }
                                TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
                                if (!text.isEmpty()) {
                                    cargoMesSectionRecord.periodo = text.reverse().join(' ')
                                }
                                if (tokens.join(' ') ==~ /.*Q\s+\d*,?\d+\.\d{2}$/) { // Sample: Q         2,612.15
                                    cargoMesSectionRecord.valor = {
                                        def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                        def currency = tokens.pop()
                                        if (currency == 'Q-') {
                                            valor = "-${valor}"
                                        }
                                        return valor
                                    }.call()
                                    cargoMesSectionRecord.nombre = tokens.join(' ')
                                    sectionRecord.productosServicios << cargoMesSectionRecord
                                } else {
                                    cargoMesSectionRecord.periodo = 'N'
                                    cargoMesSectionRecord.valor = '0'
                                    cargoMesSectionRecord.nombre = tokens.join(' ')
                                    sectionRecord.productosServicios << cargoMesSectionRecord
                                }
                                break
                            case ~/^0300150/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.totalFactura = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                break
                            case ~/^0300200/:
                                sectionRecord.aviso = line[7..-1].trim()
                                break
                            case ~/^0300300/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.totalPagar = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}".toString()
                                    }
                                    return valor
                                }.call()
                                break
                            case ~/^0400100/:
                                if (sectionRecord.containsKey('notificaciones')) { // List already exists?
                                    sectionRecord.notificaciones << line[7..-1] // Add item to list
                                } else {
                                    sectionRecord.notificaciones = [line[7..-1]] // New List with initial element
                                }

                                break
                        }
                    } catch(Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '0400200' : [ // RESUMEN***
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                 String id = line.take(LINE_ID_LENGTH)
                     try {
                         switch (id) {
//                             case ~/^0400200/:
//                                 sectionRecord.id = id
//                                 // --
//                                 def tokens = line[7..-1].tokenize()
//                                 sectionRecord.serieAdministrativa = tokens[-1]
//                                 break
//                             case ~/^0400300/:
//                                 def tokens = line[7..-1].tokenize()
//                                 while (tokens[-1] ==~ /[A-Za-z]+/) {
//                                     tokens.pop()
//                                 }
//                                 sectionRecord.numeroAdministrativo = tokens.pop()
//                                 break
                             case ~/^0400400/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.numeroAutorizacion = tokens[-1]
                                 if (sectionRecord.numeroAutorizacion == 'AUTORIZACION:') {
                                     sectionRecord.numeroAutorizacion = ' '
                                 }
                                 break
                             case ~/^0400410/:
                                 sectionRecord.facturaSerie = line[21..50].trim()
                                 if (sectionRecord.facturaSerie.isEmpty()) {
                                     sectionRecord.facturaSerie = ' '
                                 }
                                 sectionRecord.clienteNombre = line[51..-1]
                                 break
                             case ~/^0400500/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.telefono = tokens.pop()
                                 break
                             case ~/^0400600/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.mesFacturacion = tokens.pop()
                                 break
                             case ~/^0400700/:
                                 def tokens = line[7..-1].tokenize()
                                // sectionRecord.totalMes = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // 20230110 - Jasiel - Cambio para tomar valores negativos
                                sectionRecord.totalMes = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                break
                             case ~/^0400800/:
                                 def tokens = line[7..-1].tokenize()
                                 //sectionRecord.totalPagar = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                 // 20230110 - Jasiel - Cambio para tomar valores negativos
                                sectionRecord.totalPagar = {
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                    def currency = tokens.pop()
                                    if (currency == 'Q-') {
                                        valor = "-${valor}"
                                    }
                                    return valor
                                }.call()
                                break
                             case ~/^0401000/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.vencimiento = tokens.pop()
                                 break
                             case ~/^0401098/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.certificadorNIT = tokens.pop().tokenize(':').pop()
                                 tokens.remove(0)
                                 sectionRecord.certificador = tokens.join(' ')
                                 break
                             case ~/^0401099/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.numeroAdministrativo = tokens.pop()
                                 if (sectionRecord.numeroAdministrativo == 'ADMINISTRATIVO:') {
                                     sectionRecord.numeroAdministrativo = ' '
                                 }
                                 break
                             case ~/^0401100/:
                                 def tokens = line[7..-1].tokenize()
                                 sectionRecord.serieAdministrativa = tokens.pop()
                                 if (sectionRecord.serieAdministrativa == 'ADMINISTRATIVA:') {
                                     sectionRecord.serieAdministrativa = ' '
                                 }
                                 break
                         }
                     } catch(Exception e) {
                         Globals.LINE_ERROR = line
                         throw e
                     }
                }
                return sectionRecord
            }
        ],
        '0700100' : [
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.servicios = [] // new List
                String llamadasDynamicKey
                String eventosDynamicKey
                String otrosDynamicKey
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^0700100/:
                                sectionRecord.id = id
                                break
                            case ~/^0800100/:
                                // Sample:
                                // 0800100                          7952-1587
                                TxtDigester.SectionRecord telefonoSectionRecord = new TxtDigester.SectionRecord()
                                telefonoSectionRecord.telefono = line[7..-1].trim()
                                telefonoSectionRecord.telefonoV2 = telefonoSectionRecord.telefono.replace('-', '')
                                telefonoSectionRecord.consumos = [] // new List
                                //telefonoSectionRecord.consumos = [new TxtDigester.SectionRecord()] // new List with initial object
                                sectionRecord.servicios << telefonoSectionRecord
                                break
                            case ~/^(09|11)00100/:
                                // 0900100  LLAMADAS LOCALES
                                // 1100100  LLAMADAS NACIONALES | LLAMADAS A OPERADORES
                                //
                                //  *** STRANGE CASE!!!!!
                                //0800100                          7948-0333
                                //0900100  LLAMADAS LOCALES <----- Overwritten
                                //0900400  TOTAL: 0 MINUTOS LOCALES <----- Overwritten
                                //0900100  LLAMADAS LOCALES !!!!
                                //0900400  TOTAL: 47 MINUTOS LOCALES !!!!

                                // *** NOTE: 0900100 or 1100100 creates NEW ENTRY *** //
                                sectionRecord.servicios.last().consumos << new TxtDigester.SectionRecord() // new List

                                llamadasDynamicKey = line[7..-1].tokenize().join('_').toLowerCase()
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey] = new TxtDigester.SectionRecord()
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].cabeceras = []
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].consumoType = TxtSpecs.ConsumoType.LLAMADA // DESCONOCIDO
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].totales = [] // New list
                                break
                            case ~/^(09|11)00200/:
                                // -- LLAMADAS LOCALES (x3) --
                                // * 1.FECHA  2.HORA     3.DESTINO    4.MIN  5.VALOR
                                // or
                                // -- LLAMADAS NACIONALES (x2) --
                                // * 1.FECHA 2.HORA         3.DESTINO        4.LUGAR[opcional]   5.MIN 6.VALOR
                                // or
                                // -- LLAMADAS A OPERADORES (x2) --
                                // * 1.FECHA 2.HORA         3.DESTINO        4.LUGAR[opcional]   5.MIN 6.VALOR
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{1}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                Integer columnas = headers.size() / cabeceras.size()
                                Integer tokensQty = cabeceras.size()
                                if (tokensQty < 5 && tokensQty > 6) {
                                    throw new Exception("Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].cabeceras = cabeceras
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].columnas = columnas

                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].consumoType = TxtSpecs.ConsumoType.LLAMADA

                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].totales = []
                                break
/* 20230106 - Separamos el MAPEO de los registros 0900300 y 1100300 - Jasiel
                        //  case ~/^(09|11)00300/:
                                // Items sample:
                                //0900300 07/01  11:28:37 24715487     2   0.00      22/01  10:11:55 23677863     1   0.00
                                //0900300 19/01  09:46:00     1744     1   0.00      19/01  09:46:00     1744     1   0.00      19/01  09:47:00     1744     1   0.00
                                //0900300 19/01  09:49:00     1744     1   0.00      19/01  09:51:00     1744     1   0.00      19/01  10:03:00     1744     3   0.00
                                //0900300 22/01  09:12:00 77626724     1   0.00
                                // --
                                //110030022/01 08:13:27 66344111         NACIONAL             5    0.00 22/01 08:18:01 66416183         NACIONAL             2    0.00
                                //110030027/01 12:38:35 66416183         NACIONAL             2    0.00 27/01 14:18:26 66344111         NACIONAL             2    0.00
                                //110030027/01 14:20:47 66344111         NACIONAL             2    0.00
                                // --
                                //110030014/01 14:25:12 42358549         CLARO                1    0.00 14/01 14:26:06 47915719         OTRO OPERADOR        1    0.70
                                //110030014/01 14:29:34 42179639         CLARO                1    0.00 14/01 14:45:45 55529183         OTRO OPERADOR        5    3.50
                                //110030015/01 11:11:51 30408070         OTRO OPERADOR        1    0.70 15/01 11:13:10 30408070         OTRO OPERADOR        1    0.70

                        //      String lineV2 = line[7..-1]
                        //      def tokensV1 = []
                        //      String regexDelimiter = '\\d+\\.\\d{2}'
                        //      String dynamicDelimiter
                        //      while (dynamicDelimiter = lineV2.find(regexDelimiter)) {
                        //          tokensV1 << lineV2.substring(0, lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                        //          lineV2 = lineV2.substring(lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                        //      }

                        //      tokensV1.each { String tokenV1 ->
                        //          def tokensV2 = tokenV1.tokenize()

                                    // Token (VALOR)
                        //          String valor = tokensV2.pop() // Take and remove last element. Sample: 0.00
                        //          if (!(valor ==~ /\d+\.\d{2}/)) {
                        //              throw new Exception("Invalid value for 'valor': ${valor}")
                        //          }

                                    // Token (MIN)
                        //          String min = tokensV2.pop()
                        //          if (!(min ==~ /\d+/)) {
                        //              throw new Exception("Invalid value for 'min': ${valor}")
                        //          }

                                    // Token (FECHA)
                        //          String fecha = tokensV2.first()
                        //          tokensV2.remove(0) // Remove first element
                        //          if (!(fecha ==~ /\d{2}\/\d{2}/)) {
                        //              throw new Exception("Invalid value for 'fecha': ${valor}")
                        //          }

                                    // Token (HORA)
                        //          String hora = tokensV2.first()
                        //          tokensV2.remove(0) // Remove first element
                        //          if (!(hora ==~ /\d{2}:\d{2}:\d{2}/)) {
                        //              throw new Exception("Invalid value for 'hora': ${valor}")
                        //          }

                                    // Token (DESTINO)
                        //          String destino = tokensV2.first()
                        //          tokensV2.remove(0) // Remove first element

                                    // Token (LUGAR)[opcional]
                        //          String lugar = tokensV2.join(' ') // Join remaining elements

                                    // Object
                        //          TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                        //          consumoSectionRecord.fecha = fecha
                        //          consumoSectionRecord.hora = hora
                        //          consumoSectionRecord.destino = destino
                        //          consumoSectionRecord.lugar = lugar
                        //          consumoSectionRecord.min = min
                        //          consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                        //          sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].detalleConsumos << consumoSectionRecord
                        //      }
                        //      break
*/
// 20230106 - Separamos el MAPEO de los registros 0900300 - Jasiel
                            case ~/^(09)00300/:
                                // Items sample:
                                //0900300 07/01  11:28:37 24715487     2   0.00      22/01  10:11:55 23677863     1   0.00
                                //0900300 19/01  09:46:00     1744     1   0.00      19/01  09:46:00     1744     1   0.00      19/01  09:47:00     1744     1   0.00
                                //0900300 19/01  09:49:00     1744     1   0.00      19/01  09:51:00     1744     1   0.00      19/01  10:03:00     1744     3   0.00
                                //0900300 22/01  09:12:00 77626724     1   0.00

                                String lineV2 = line[7..-1]
                                def tokensV1 = []
                                String regexDelimiter = '\\d+\\.\\d{2}'
                                String dynamicDelimiter
                                while (dynamicDelimiter = lineV2.find(regexDelimiter)) {
                                    tokensV1 << lineV2.substring(0, lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                    lineV2 = lineV2.substring(lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                }

                                tokensV1.each { String tokenV1 ->
                                    def tokensV2 = tokenV1.tokenize()

                                    // Token (VALOR)
                                    String valor = tokensV2.pop() // Take and remove last element. Sample: 0.00
                                    if (!(valor ==~ /\d+\.\d{2}/)) {
                                        throw new Exception("Invalid value for 'valor': ${valor}")
                                    }

                                    // Token (MIN)
                                    String min = tokensV2.pop()
                                    if (!(min ==~ /\d+/)) {
                                        throw new Exception("Invalid value for 'min': ${valor}")
                                    }

                                    // Token (FECHA)
                                    String fecha = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(fecha ==~ /\d{2}\/\d{2}/)) {
                                        throw new Exception("Invalid value for 'fecha': ${valor}")
                                    }

                                    // Token (HORA)
                                    String hora = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(hora ==~ /\d{2}:\d{2}:\d{2}/)) {
                                        throw new Exception("Invalid value for 'hora': ${valor}")
                                    }

                                    // Token (DESTINO)
                                    String destino = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element

                                    // Token (LUGAR)[opcional]
                                    String lugar = tokensV2.join(' ') // Join remaining elements

                                    // Object
                                    TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                                    consumoSectionRecord.fecha = fecha
                                    consumoSectionRecord.hora = hora
                                    consumoSectionRecord.destino = destino
                                    consumoSectionRecord.lugar = lugar
                                    consumoSectionRecord.min = min
                                    consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                                    sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].detalleConsumos << consumoSectionRecord
                                }
                                break
// 20230106 - Separamos el MAPEO de los registros 1100300 - Jasiel
                            case ~/^(11)00300/:
                                // Items sample:
                                //110030022/01 08:13:27 66344111         NACIONAL             5    0.00 22/01 08:18:01 66416183         NACIONAL             2    0.00
                                //110030027/01 12:38:35 66416183         NACIONAL             2    0.00 27/01 14:18:26 66344111         NACIONAL             2    0.00
                                //110030027/01 14:20:47 66344111         NACIONAL             2    0.00
                                // --
                                //110030014/01 14:25:12 42358549         CLARO                1    0.00 14/01 14:26:06 47915719         OTRO OPERADOR        1    0.70
                                //110030014/01 14:29:34 42179639         CLARO                1    0.00 14/01 14:45:45 55529183         OTRO OPERADOR        5    3.50
                                //110030015/01 11:11:51 30408070         OTRO OPERADOR        1    0.70 15/01 11:13:10 30408070         OTRO OPERADOR        1    0.70

                                String lineV2 = line[7..-1]
                                def tokensV1 = []
                                String regexDelimiter = '\\d+\\.\\d{2}'
                                String dynamicDelimiter
                                while (dynamicDelimiter = lineV2.find(regexDelimiter)) {
                                    tokensV1 << lineV2.substring(0, lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                    lineV2 = lineV2.substring(lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                }

                                tokensV1.each { String tokenV1 ->
                                	String tokenVA = tokenV1[-13..-1]
                                	String tokenVB = tokenV1[0..49].trim()
                                    def tokensV2 = tokenVB.tokenize()

                                    // Token (VALOR)
                                    //String valor = tokensV2.pop() // Take and remove last element. Sample: 0.00
                                    //if (!(valor ==~ /\d+\.\d{2}/)) {
                                    //	throw new Exception("Invalid value for 'valor': ${valor}")
                                    //}

                                    // Token (MIN)
                                    //String min = tokensV2.pop()
                                    //if (!(min ==~ /\d+/)) {
                                    //	throw new Exception("Invalid value for 'min': ${valor}")
                                    //}

                                    // Token (VALOR)
                                    String valor = tokenVA[5..-1].replace(',','').trim()
                                    if (!(valor ==~ /\d+\.\d{2}/)) {
                                    //if (!(valor ==~ /(\d+\,)?(\d+\.\d{2})/)) {
                                    	throw new Exception("Invalid value for 'valor': ${valor}")
                                    }
                                    
				                    // Token (MIN)
                                    String min = tokenVA[0..4].trim()
                                    if (!(min ==~ /\d+/)) {
                                    	//throw new Exception("Invalid value for 'min': ${valor}")
                                    	throw new Exception("Invalid value for 'min': ${min}")
                                    }

                                    // Token (FECHA)
                                    String fecha = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(fecha ==~ /\d{2}\/\d{2}/)) {
                                        throw new Exception("Invalid value for 'fecha': ${valor}")
                                    }

                                    // Token (HORA)
                                    String hora = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(hora ==~ /\d{2}:\d{2}:\d{2}/)) {
                                        throw new Exception("Invalid value for 'hora': ${valor}")
                                    }

                                    // Token (DESTINO)
                                    String destino = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element

                                    // Token (LUGAR)[opcional]
                                    String lugar = tokensV2.join(' ') // Join remaining elements

                                    // Object
                                    TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                                    consumoSectionRecord.fecha = fecha
                                    consumoSectionRecord.hora = hora
                                    consumoSectionRecord.destino = destino
                                    consumoSectionRecord.lugar = lugar
                                    consumoSectionRecord.min = min
                                    consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                                    sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].detalleConsumos << consumoSectionRecord
                                }
                                break
////////////////////////////
                            case ~/^(09|11)00400/:
                                // Sample:
                                // 0900400  TOTAL: 126 MINUTOS LOCALES
                                // 0900400  126 MINUTOS LOCALES CON VALOR  DE Q0.00
                                // 0900400  0 MINUTOS LOCALES CON VALOR  DE Q0.00
                                //sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].totales << line[7..-1].trim()
                                try {
                                    sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].totales << line[7..-1].trim()
                                } catch(Exception ignored) {
                                    println "EXCEPTION TELEFONO: ${sectionRecord.servicios.last()telefono}"
                                }
                                break
                            case ~/^(09|11)00500/:
                                // Sample:
                                // 0900500  126 MINUTOS LOCALES CON VALOR  DE Q0.00
                                // 0900500  0 MINUTOS LOCALES CON VALOR  DE Q0.00
                                sectionRecord.servicios.last().consumos.last()[llamadasDynamicKey].totales << line[7..-1].trim()
                                break
                            case ~/^1201100/:
                                // *** NOTE: 1201100 creates NEW ENTRY *** //
                                sectionRecord.servicios.last().consumos << new TxtDigester.SectionRecord() // new List

                                eventosDynamicKey = line[7..-1].replace(':', '').tokenize().join('_').toLowerCase()
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey] = new TxtDigester.SectionRecord()
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].cabeceras = []
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].consumoType = TxtSpecs.ConsumoType.DESCONOCIDO
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].totales = []
                                break
                            case ~/^1201200/:
                                // -- OTROS CARGOS CLARO TV-PPV (x2) --
                                // * 1.FECHA 2.HORA         3.DESCRIPCION               4.TARIFA     5.VALOR
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{1}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                Integer columnas = headers.size() / cabeceras.size()
                                Integer tokensQty = cabeceras.size()
                                if (tokensQty != 5) {
                                    throw new Exception("Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].cabeceras = cabeceras
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].columnas = columnas

                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].consumoType = TxtSpecs.ConsumoType.EVENTO

                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].totales = []
                                break
                            case ~/^1201300/:
                                // Items sample:
                                // 120130024/04 23:00:00 Ballet Extremo                NORMAL     45.00

                                String lineV2 = line[7..-1]
                                def tokensV1 = []
                                String regexDelimiter = '\\d+\\.\\d{2}'
                                String dynamicDelimiter
                                while (dynamicDelimiter = lineV2.find(regexDelimiter)) {
                                    tokensV1 << lineV2.substring(0, lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                    lineV2 = lineV2.substring(lineV2.indexOf(dynamicDelimiter) + dynamicDelimiter.length())
                                }

                                tokensV1.each { String tokenV1 ->
                                    def tokensV2 = tokenV1.tokenize()

                                    // Token (VALOR)
                                    String valor = tokensV2.pop() // Take and remove last element. Sample: 0.00
                                    if (!(valor ==~ /\d+\.\d{2}/)) {
                                        throw new Exception("Invalid value for 'valor': ${valor}")
                                    }

                                    // Token (TARIFA)
                                    String tarifa = tokensV2.pop()

                                    // Token (FECHA)
                                    String fecha = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(fecha ==~ /\d{2}\/\d{2}/)) {
                                        throw new Exception("Invalid value for 'fecha': ${valor}")
                                    }

                                    // Token (HORA)
                                    String hora = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(hora ==~ /\d{2}:\d{2}:\d{2}/)) {
                                        throw new Exception("Invalid value for 'hora': ${valor}")
                                    }

                                    // Token (DESCRIPCION)
                                    String descripcion = tokensV2.join(' ') // Join remaining elements

                                    // Object
                                    TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                                    consumoSectionRecord.fecha = fecha
                                    consumoSectionRecord.hora = hora
                                    consumoSectionRecord.descripcion = descripcion
                                    consumoSectionRecord.tarifa = tarifa
                                    consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                                    sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].detalleConsumos << consumoSectionRecord
                                }
                                break
                            case ~/^1201400/:
                                // Sample:
                                // 1201400  TOTAL: 1 EVENTOS CON VALOR  DE   Q45.00
                                sectionRecord.servicios.last().consumos.last()[eventosDynamicKey].totales << line[7..-1].trim()
                                break
                            case ~/^1300100/:
                                // *** NOTE: 1300100 creates NEW ENTRY *** //
                                sectionRecord.servicios.last().consumos << new TxtDigester.SectionRecord() // new List

                                otrosDynamicKey = line[7..-1].replace(':', '').tokenize().join('_').toLowerCase()
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey] = new TxtDigester.SectionRecord()
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].cabeceras = []
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].consumoType = TxtSpecs.ConsumoType.DESCONOCIDO
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].totales = []
                                break
                            case ~/^1300200/:
                                // OTROS SERVICIOS(x2)
                                // * 1.TELEFONO    2.FECHA 3.DESCRIPCION                4.VALOR
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{1}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                Integer columnas = headers.size() / cabeceras.size()
                                Integer tokensQty = cabeceras.size()
                                if (tokensQty != 4) {
                                    throw new Exception("Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].cabeceras = cabeceras
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].columnas = columnas

                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].consumoType = TxtSpecs.ConsumoType.OTRO

                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].detalleConsumos = []
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].totales = []
                                break
                            case ~/^1300300/:
                                // Items sample:
                                //1300300 002234-7425 02/01 DETALLE.LLAMADAS.LOCALES    5.00
                                //1300300 007772-2183 17/07 CONVENIO.18MESES 01 de 18   47.34
                                //1300300 002508-4037 02/03 SMART.TV40.35M   01 de 35   180.00

                                String lineV2 = line[7..-1]
                                def tokensV1 = []
                                if (line.length() > 70) {
                                    tokensV1 << line[7..70]
                                    tokensV1 << line[71..-1]
                                } else {
                                    tokensV1 << line[7..-1]
                                }
                                tokensV1.each { String tokenV1 ->
                                    def tokensV2 = tokenV1.tokenize()

                                    // Token (VALOR)
                                    String valor = FormatUtil.currencyToNumberFormatter(tokensV2.pop()) // Take and remove last element. Sample: 0.00
                                    if (!(valor ==~ /-?\d+\.\d{2}/)) {
                                        throw new Exception("Invalid value for 'valor': ${valor}")
                                    }

                                    // Token (TELEFONO)
                                    String telefono = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
//                                    if (!(telefono ==~ /\d{2}:\d{2}:\d{2}/)) {
//                                        throw new Exception("Invalid value for 'telefono': ${valor}")
//                                    }

                                    // Token 1 (FECHA)
                                    String fecha = tokensV2.first()
                                    tokensV2.remove(0) // Remove first element
                                    if (!(fecha ==~ /\d{2}\/\d{2}/)) {
                                        throw new Exception("Invalid value for 'fecha': ${valor}")
                                    }

                                    // Token 3 (DESCRIPCION)
                                    String descripcion = tokensV2.join(' ')
                                    //tokensV2.remove(0) // Remove first element

                                    // Object
                                    TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                                    consumoSectionRecord.telefono = telefono
                                    consumoSectionRecord.fecha = fecha
                                    consumoSectionRecord.descripcion = descripcion
                                    consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                                    sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].detalleConsumos << consumoSectionRecord
                                }
                                break
                            case ~/^1300400/:
                                // 1300400  TOTAL:OTROS SERVICIOS 5.00
                                sectionRecord.servicios.last().consumos.last()[otrosDynamicKey].totales << line[7..-1].trim()
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '1400100' : [ // RESUMEN DE INTEGRADOS
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.resumenIntegrados = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^1400100/:
                                sectionRecord.id = id
                                break
                            case ~/^1400200/:
                                // RESUMEN DE INTEGRADOS(x1)
                                // * 1.TELEFONO          2.CABLE     3.TURBONET        4.CUOTA   5.MIN. LOCAL          6.DIA          7.DNA    8.MIN. OPER        9.OTROS        10.TOTAL
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                Integer columnas = headers.size() / cabeceras.size()
                                Integer tokensQty = cabeceras.size()
                                if (tokensQty != 10) {
                                    throw new Exception("RESUMEN DE INTEGRADOS. Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.cabeceras = cabeceras
                                sectionRecord.columnas = columnas
                                break
                            case ~/^1400(3|5)00/: // Consumos | TOTALES(last line)
                                String lineV2 = line[7..-1]
                                def tokens = lineV2.tokenize()
                                if (tokens.size() != 10) {
                                    throw new Exception("RESUMEN DE INTEGRADOS Item. Cantidad de tokens no esperada: ${tokensQty}")
                                }

                                // Token (TELEFONO)
                                String telefono = tokens.first()
                                tokens.remove(0) // Remove first element

                                // Token (CABLE)
                                String cable = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (TURBONET)
                                String turbonet = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (CUOTA)
                                String cuota = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (MIN. LOCAL)
                                String minLocal = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (DIA)
                                String dia = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (DNA)
                                String dna = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (MIN. OPER)
                                String minOper = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (OTROS)
                                String otros = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Token (TOTAL)
                                String total = FormatUtil.currencyToNumberFormatter(tokens.first())
                                tokens.remove(0)

                                // Object
                                TxtDigester.SectionRecord resumenIntegradoSectionRecord = new TxtDigester.SectionRecord()
                                resumenIntegradoSectionRecord.telefono = telefono
                                resumenIntegradoSectionRecord.cable = cable
                                resumenIntegradoSectionRecord.turbonet = turbonet
                                resumenIntegradoSectionRecord.cuota = cuota
                                resumenIntegradoSectionRecord.minLocal = minLocal
                                resumenIntegradoSectionRecord.dia = dia
                                resumenIntegradoSectionRecord.dna = dna
                                resumenIntegradoSectionRecord.minOper = minOper
                                resumenIntegradoSectionRecord.otros = otros
                                resumenIntegradoSectionRecord.total = total
                                // --
                                if (telefono == 'TOTALES') {
                                    sectionRecord.totales = resumenIntegradoSectionRecord
                                } else { // ==~ /\d+/
                                    sectionRecord.resumenIntegrados << resumenIntegradoSectionRecord
                                }
                                break
                            case ~/^1400600/: // NUMERO DE LINEAS
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.numeroLineas = tokens.pop().toInteger()
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '1401100' : [ // DETALLE DE CARGOS
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.cargos = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^1401100/:
                                sectionRecord.id = id
                                sectionRecord.titulo = line[7..-1].trim()
                                break
                            case ~/^1401200/:
                                sectionRecord.nombre = line[7..-1].trim()
                                break
                            case ~/^1401210/:
                                sectionRecord.subNombre = line[7..-1].trim()
                                break
                            case ~/^1401300/:
                                // DETALLE DE CARGOS [headers in 2 lines]
                                // 1401300 1.Telefono   2.Minutos Cursados            3.Periodo   4.Minutos     5.Por                    6.Costo     7.Total
                                // 1401310                                                        4.Sin Costo   5.Pagar                  6.Unitario
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    cabeceras.add(it.trim())
                                }
                                int columnas = headers.size() / cabeceras.size()
                                int tokensQty = cabeceras.size()
                                if (tokensQty == 7) {
                                    sectionRecord.cabeceras = cabeceras
                                    sectionRecord.columnas = columnas
                                } else {
                                    throw new Exception("DETALLE DE CARGOS (headers 1st line). Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                break
                            case ~/^1401310/:
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    cabeceras.add(it.trim())
                                }
                                int tokensQty = cabeceras.size()
                                if (tokensQty == 3) { // Second line
                                    sectionRecord.cabeceras[3] = "${sectionRecord.cabeceras[3]} ${cabeceras[0]}"
                                    sectionRecord.cabeceras[4] = "${sectionRecord.cabeceras[4]} ${cabeceras[1]}"
                                    sectionRecord.cabeceras[5] = "${sectionRecord.cabeceras[5]} ${cabeceras[2]}"
                                } else {
                                    throw new Exception("DETALLE DE CARGOS (headers 2nd line). Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                break
                            case ~/^1401500/:
                                // SAMPLES:
                                // 14015000079621250 ON NET                    0 FEB/2021            0           0              0.00    Q0.00
                                // 14015000079621250 NACIONAL                 67 FEB/2021           67           0              0.00    Q0.00
                                // 14015000079621250 OPERADOR              2,173 FEB/2021        2,173           0              0.00    Q0.00
                                // 14015000079621250 LOCAL                   198 FEB/2021          198           0              0.00    Q0.00
                                // Object
                                def tokens = line[7..-1].tokenize()
                                def total = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                def costoUnitario = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                def porPagar = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                def minutosSinCosto = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                def periodo = tokens.pop()
                                def telefono = tokens[0]; tokens.remove(0)
                                //def minutosCursados = tokens.join(' ')
                                def minutosCursados = line[18..44]

                                TxtDigester.SectionRecord cargoSectionRecord = new TxtDigester.SectionRecord()
                                cargoSectionRecord.telefono = telefono
                                cargoSectionRecord.minutosCursados = minutosCursados
                                cargoSectionRecord.periodo = periodo
                                cargoSectionRecord.minutosSinCosto = minutosSinCosto
                                cargoSectionRecord.porPagar = porPagar
                                cargoSectionRecord.costoUnitario = costoUnitario
                                cargoSectionRecord.total = total
                                //
                                sectionRecord.cargos << cargoSectionRecord
                                break
                            case ~/^1401600/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.totales = new TxtDigester.SectionRecord()
                                sectionRecord.totales.total = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                sectionRecord.totales.porPagar = tokens.pop()
                                sectionRecord.totales.minutosSinCosto = tokens.pop()
                                sectionRecord.totales.minutosCursados = tokens.pop()
                                sectionRecord.totales.descripcion = tokens.pop()
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '1800100' : [ // DETALLE DE ENLACES
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.enlaces = [] // new List
                sectionRecord.totales = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^1800100/:
                                sectionRecord.id = id
                                sectionRecord.titulo = line[7..-1].trim()
                                break
                            case ~/^1800200/:
                                sectionRecord.fecha = line[7..-1].trim()
                                break
                            case ~/^1800300/:

                                break
                            case ~/^1800400/:
                                // DETALLE DE ENLACES(x1) [headers in 2 lines]
                                // 1800400 1.Codigo     2.Codigo  3.Descripcion servicio             4.Direccion completa             5.Cargo                6.Valor($)   7.Valor(Q)
                                // 1800410 1.Enlace     2.Serv.
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    //if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    //}
                                }
                                int columnas = headers.size() / cabeceras.size()
                                int tokensQty = cabeceras.size()
                                if (tokensQty == 7) { // First line
                                    sectionRecord.cabeceras = cabeceras
                                    sectionRecord.columnas = columnas
                                } else {
                                    throw new Exception("DETALLE DE ENLACES (headers 1st line). Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                break
                            case ~/^1800410/:
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    cabeceras.add(it.trim())
                                }
                                int tokensQty = cabeceras.size()
                                if (tokensQty == 2) { // Second line
                                    sectionRecord.cabeceras[0] = "${sectionRecord.cabeceras[0]} ${cabeceras[0]}"
                                    sectionRecord.cabeceras[1] = "${sectionRecord.cabeceras[1]} ${cabeceras[1]}"
                                } else {
                                    throw new Exception("DETALLE DE ENLACES (headers 2nd line). Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                break
                            case ~/^1800500/:
                                // Sample [address in 2 lines]:
                                // 1800500 603100001 5533 INTERNET CORPORATIVO LOCAL 40 MBPS  INTERNET_9 AVENIDA 22-00 ZONA  CARGO MENSUAL        0.00       2,400.00
                                // 1800510                                                    1 GUATEMALA

                                // Object
                                TxtDigester.SectionRecord enlaceSectionRecord = new TxtDigester.SectionRecord()
                                enlaceSectionRecord.codigoEnlace = line[7..17].trim()
                                enlaceSectionRecord.codigoServ = line[18..22].trim()
                                enlaceSectionRecord.descripcionServicio = line[23..58].trim()
                                enlaceSectionRecord.direccionCompleta = line[59..89].trim()
                                enlaceSectionRecord.cargo = line[90..110].trim()
                                enlaceSectionRecord.valorS = FormatUtil.currencyToNumberFormatter(line[111..121])
                                enlaceSectionRecord.valorQ = FormatUtil.currencyToNumberFormatter(line[122..-1])
                                //
                                sectionRecord.enlaces << enlaceSectionRecord
                                break
                            case ~/^1800510/:
                                sectionRecord.enlaces.last().direccionCompleta = "${sectionRecord.enlaces.last().direccionCompleta} ${line[7..-1].trim()}"
                                break
                            case ~/^1800600/:
                                // Sample (address part of 1800500):
                                // 1800600                                                    1 GUATEMALA
                                sectionRecord.enlaces.last().direccionCompleta += " ${line[7..-1].trim()}"
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '1600100' : [ // DETALLE DE FINANCIAMIENTOS
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.financiamientos = [] // new List
                Boolean newProductList = false
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^1600100/:
                              sectionRecord.id = id
                              sectionRecord.titulo = line[7..-1].trim()
                              break
                            case ~/^1600200/: // RESUMEN CONCEPTO DE COBRO
                                // DETALLE DE FINANCIAMIENTOS(x1)
                                // * 1.Producto                         2.Serie                     3.Financiamiento               4.Saldo         5.Valor Cuota 6.No.Cuota
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                int columnas = headers.size() / cabeceras.size()
                                int tokensQty = cabeceras.size()
                                if (tokensQty != 6) {
                                    throw new Exception("DETALLE DE FINANCIAMIENTOS. Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.cabeceras = cabeceras
                                sectionRecord.columnas = columnas

                                sectionRecord.financiamientos << new TxtDigester.SectionRecord()
                                sectionRecord.financiamientos.last().productos = []
                                break
                            case ~/^1600300/:
                                // Items samples:
                                // 1600300 Convenio por COVID-19          0000005693                 Q             0.06  Q             0.00  Q             0.06    1/1
                                // 1600300 Convenio por COVID-19          0000005693                 Q         2,190.60  Q         1,095.30  Q           182.55    6/12
                                // 1600300 BUNDLE ALL IN ONE HP 22B402TLA 8CC9420P0X                 Q         3,468.00  Q         2,023.00  Q           289.00    5/12
                                // 1600300 REDONDEO CUOTA FINANCIAMIENTO                             Q            11.00  Q             0.00  Q            11.00    1/1
                                // 1600300 FINANCIAMIENTOVARIABLE24                          FINANCIAMIENTOVARIABLE24  Q  5,472.00  Q  5,016.00  Q    228.00       2/24

                                // Token (Producto)
                                //String producto = line[8..38].trim()
                                String producto = line[8..57].trim()

                                // Token (Serie)
                                //String serie = line[39..65].trim()
                                String serie = line[58..82].trim()

                                //def tokens = line[66..-1].split(/\s{2}/).findAll({it.trim() != 'Q'}).findAll()
                                def tokens = line[83..-1].split(/\s{2}/).findAll({it.trim() != 'Q'}).findAll()
                                if (tokens.size() != 4) {
                                    throw new Exception("DETALLE DE FINANCIAMIENTOS Item. Cantidad de tokens no esperada: ${tokensQty}")
                                }

                                // Token (No.Cuota)
                                String numCuota = tokens.pop()
                                // TODO: Validate token

                                // Token (Valor Cuota)
                                String valorCuota = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Token (Saldo)
                                String saldo = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Token (Financiamiento)
                                String financiamiento = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Object
                                TxtDigester.SectionRecord productoSectionRecord = new TxtDigester.SectionRecord()
                                productoSectionRecord.id = id
                                productoSectionRecord.producto = producto.trim()
                                productoSectionRecord.serie = serie
                                productoSectionRecord.financiamiento = financiamiento
                                productoSectionRecord.saldo = saldo
                                productoSectionRecord.valorCuota = valorCuota
                                productoSectionRecord.numCuota = numCuota
                                // --
                                //TxtDigester.SectionRecord itemSectionRecord = new TxtDigester.SectionRecord()
                                //itemSectionRecord.producto = productoSectionRecord
                                //sectionRecord.financiamientos.last().productos << productoSectionRecord

                                if (!newProductList) {
                                    newProductList = true
                                    sectionRecord.financiamientos.last().productos << new TxtDigester.SectionRecord()
                                    sectionRecord.financiamientos.last().productos.last().productList = [productoSectionRecord] // new List with initial item
                                } else {
                                    sectionRecord.financiamientos.last().productos.last().productList << productoSectionRecord // new List with initial item
                                }
                                break
                            case ~/^1600400/: // TOTAL XYZ
                                // Items samples:
                                // 2000400 TOTAL LINEA                                               Q             0.06  Q             0.00  Q             0.06
                                // 2000400 TOTAL CONVENIO                                            Q         2,190.60  Q         1,095.30  Q           182.55
                                def tokens = line[7..-1].split(/\s{2}/).findAll({it.trim() != 'Q'}).findAll()
                                if (tokens.size() != 4) {
                                    throw new Exception("DETALLE DE FINANCIAMIENTOS Total Item. Cantidad de tokens no esperada: ${tokensQty}")
                                }

                                // Token (Valor Cuota)
                                String valorCuota = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Token (Saldo)
                                String saldo = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Token (Financiamiento)
                                String financiamiento = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                // TODO: Validate token

                                // Token (Producto)
                                String totalConcepto = tokens.pop()

                                // Object
                                TxtDigester.SectionRecord totalSectionRecord = new TxtDigester.SectionRecord()
                                totalSectionRecord.id = id
                                totalSectionRecord.concepto = totalConcepto.trim()
                                totalSectionRecord.financiamiento = financiamiento
                                totalSectionRecord.saldo = saldo
                                totalSectionRecord.valorCuota = valorCuota
                                // --
                                //sectionRecord.totales << financiamientoSectionRecord

                                // Update last itemSectionRecord
                                //sectionRecord.financiamientos.last().total = totalSectionRecord
                                //sectionRecord.financiamientos.last().productos << totalSectionRecord
                                sectionRecord.financiamientos.last().productos.last().productList << totalSectionRecord

                                newProductList = false

                                break
                            case ~/^1600500/: // TOTAL FINANCIAMIENTOS DEL MES
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.totalFinanciamientoMes = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
        '3000000' : [ // NOTAS DE ABONADO
              parse : { List<String> lines ->
                  TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                  sectionRecord.notasAbonado = [] // new List
                  lines.each { line ->
                      String id = line.take(LINE_ID_LENGTH)
                      try {
                          switch (id) {
                              case ~/^3000000/:
                                  sectionRecord.id = id
                                  break
                              case ~/^3000100/:
                                  //TxtDigester.SectionRecord notasAbonadoSectionRecord = new TxtDigester.SectionRecord()
                                  //notasAbonadoSectionRecord.detalles = []
                                  sectionRecord.notasAbonado << new TxtDigester.SectionRecord() // new NotaAbonado

                                  TxtDigester.SectionRecord notaSectionRecord = new TxtDigester.SectionRecord()
                                  def tokens = line[7..-1].tokenize()
                                  notaSectionRecord.numeroAutorizacion = tokens.pop()
                                  notaSectionRecord.fechaEmision = tokens.pop()
                                  notaSectionRecord.numero = tokens.pop()
                                  notaSectionRecord.serie = tokens.pop()

                                  sectionRecord.notasAbonado.last().nota = notaSectionRecord
                                  break
                              case ~/^3000210/: // Cliente
                                  sectionRecord.notasAbonado.last().cliente = new TxtDigester.SectionRecord()
                                  // --
                                  sectionRecord.notasAbonado.last().cliente.nombre = line[7..-1].trim()
                                  break
                              case ~/^3000220/:
                                  sectionRecord.notasAbonado.last().cliente.direccion = line[7..-1].trim()
                                  break
                              case ~/^3000230/:
                                  def _dirL2 = line[7..-1] // Parte de la dirección
                                  sectionRecord.notasAbonado.last().cliente.direccion += _dirL2
                                  break
                              case ~/^3000240/:
                                  sectionRecord.notasAbonado.last().cliente.datoX = 'datoX'
                                  def _dirL3 = line[7..-1] // Parte de la dirección
                                  sectionRecord.notasAbonado.last().cliente.direccion += _dirL3
                                  break
                              case ~/^3000250/:
                                  sectionRecord.notasAbonado.last().cliente.datoY = 'datoY'
                                  sectionRecord.notasAbonado.last().cliente.nit = line[7..-1].trim()
                                  break
                              case ~/^3000260/:
                                  sectionRecord.notasAbonado.last().cliente.telefono = line[7..-1].trim()
                                  break
                              case ~/^3000300/: // Factura Asociada
                                  TxtDigester.SectionRecord facturaAsociada = new TxtDigester.SectionRecord()
                                  sectionRecord.notasAbonado.last().facturaAsociada = facturaAsociada

                                  def tokens = line[7..-1].tokenize()
                                  facturaAsociada.monto = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                  tokens.pop() // Remove 'Q'
                                  def telefono = tokens.pop()
                                  if (telefono.contains(',')) {
                                      facturaAsociada.telefono = telefono[-6..-1]
                                      facturaAsociada.numero = telefono[-0..-7]
                                  } else {
                                      facturaAsociada.telefono = telefono
                                      facturaAsociada.numero = tokens.pop()
                                  }
                                  facturaAsociada.serie = tokens.pop()
                                  facturaAsociada.fecha = tokens.pop()
                                  break
                              case ~/^3000410/: // Certificador
                                  sectionRecord.notasAbonado.last().certificador = new TxtDigester.SectionRecord()
                                  // --
                                  def tokens = line[7..-1].tokenize()
                                  sectionRecord.notasAbonado.last().certificador.nit = tokens.pop().tokenize(':')[-1]
                                  sectionRecord.notasAbonado.last().certificador.nombre = tokens.join(' ')
                                  break
                              case ~/^3000420/:
                                  def tokens = line[7..-1].tokenize()
                                  sectionRecord.notasAbonado.last().certificador.serieAdministrativa = tokens[-1]
                                  break
                              case ~/^3000430/:
                                  def tokens = line[7..-1].tokenize()
                                  sectionRecord.notasAbonado.last().certificador.numeroAdministrativo = tokens[-1]
                                  break
                              case ~/^3000500/: // Detalles
                                  sectionRecord.notasAbonado.last().detalles = [] // new List
                                  break
                              case ~/^3000510/:
                                  TxtDigester.SectionRecord detalleSectionRecord = new TxtDigester.SectionRecord()
                                  sectionRecord.notasAbonado.last().detalles << detalleSectionRecord
                                  // --
                                  def tokens = line[7..-1].tokenize()
                                  detalleSectionRecord.valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                  tokens.pop() // Remove 'Q'
                                  detalleSectionRecord.concepto = tokens.join(' ')
                                  break
                              case ~/^3000600/:
                                  TxtDigester.SectionRecord totalSectionRecord = new TxtDigester.SectionRecord()
                                  sectionRecord.notasAbonado.last().total = totalSectionRecord
                                  // --
                                  def tokens = line[7..-1].tokenize()
                                  totalSectionRecord.valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                  tokens.pop() // Remove 'Q'
                                  totalSectionRecord.concepto = tokens.join(' ')
                                  break
                              case ~/^3000610/:
                                  sectionRecord.notasAbonado.last().totalLetras = line[7..-1].trim().replaceAll('\\s{2,}', ' ') // Replace multiple blank spaces with a single one
                                  break
                              case ~/^3000700/:
                                  sectionRecord.notasAbonado.last().motivo = line[7..-1]
                                  break
                          }
                      } catch (Exception e) {
                          Globals.LINE_ERROR = line
                          throw e
                      }
                  }
                  return sectionRecord
            }
        ],
        '3200000' : [ // NOTAS DE CREDITO
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.notasCredito = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^3200000/:
                                sectionRecord.id = id
                                break
                            case ~/^3200100/:
                                TxtDigester.SectionRecord notasAbonadoSectionRecord = new TxtDigester.SectionRecord()
                                notasAbonadoSectionRecord.detalles = []
                                sectionRecord.notasCredito << new TxtDigester.SectionRecord() // new NotaAbonado

                                TxtDigester.SectionRecord notaSectionRecord = new TxtDigester.SectionRecord()
                                def tokens = line[7..-1].tokenize()
                                notaSectionRecord.numeroAutorizacion = tokens.pop()
                                notaSectionRecord.fechaEmision = tokens.pop()
                                notaSectionRecord.numero = tokens.pop()
                                notaSectionRecord.serie = tokens.pop()

                                sectionRecord.notasCredito.last().nota = notaSectionRecord
                                break
                            case ~/^3200210/: // Cliente
                                sectionRecord.notasCredito.last().cliente = new TxtDigester.SectionRecord()
                                //--
                                sectionRecord.notasCredito.last().cliente.nombre = line[7..-1].trim()
                                break
                            case ~/^3200220/:
                                sectionRecord.notasCredito.last().cliente.direccion = line[7..-1].trim()
                                break
                            case ~/^3200230/:
                                def _dirL2 = line[7..-1] // Parte de la dirección
                                sectionRecord.notasCredito.last().cliente.direccion += _dirL2
                                break
                            case ~/^3200240/:
                                sectionRecord.notasCredito.last().cliente.datoX = 'datoX'
                                def _dirL3 = line[7..-1] // Parte de la dirección
                                sectionRecord.notasCredito.last().cliente.direccion += _dirL3
                                break
                            case ~/^3200250/:
                                sectionRecord.notasCredito.last().cliente.datoY = 'datoY'
                                sectionRecord.notasCredito.last().cliente.nit = line[7..-1].trim()
                                break
                            case ~/^3200260/:
                                sectionRecord.notasCredito.last().cliente.telefono = line[7..-1].trim()
                                break
                            case ~/^3200300/: // Factura Asociada
                                TxtDigester.SectionRecord facturaAsociada = new TxtDigester.SectionRecord()
                                sectionRecord.notasCredito.last().facturaAsociada = facturaAsociada

                                def tokens = line[7..-1].tokenize()
                                facturaAsociada.monto = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                tokens.pop() // Remove 'Q'
                                def telefono = tokens.pop()
                                if (telefono.contains(',')) {
                                    facturaAsociada.telefono = telefono[-6..-1]
                                    facturaAsociada.numero = telefono[-0..-7]
                                } else {
                                    facturaAsociada.telefono = telefono
                                    facturaAsociada.numero = tokens.pop()
                                }
                                facturaAsociada.serie = tokens.pop()
                                facturaAsociada.fecha = tokens.pop()
                                break
                            case ~/^3200410/: // Certificador
                                sectionRecord.notasCredito.last().certificador = new TxtDigester.SectionRecord()
                                // --
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.notasCredito.last().certificador.nit = tokens.pop().tokenize(':')[-1]
                                sectionRecord.notasCredito.last().certificador.nombre = tokens.join(' ')
                                break
                            case ~/^3200420/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.notasCredito.last().certificador.serieAdministrativa = tokens[-1]
                                break
                            case ~/^3200430/:
                                def tokens = line[7..-1].tokenize()
                                sectionRecord.notasCredito.last().certificador.numeroAdministrativo = tokens[-1]
                                break
                            case ~/^3200500/: // Detalles
                                sectionRecord.notasCredito.last().detalles = [] // new List
                                break
                            case ~/^3200510/:
                                TxtDigester.SectionRecord detalleSectionRecord = new TxtDigester.SectionRecord()
                                sectionRecord.notasCredito.last().detalles << detalleSectionRecord
                                // --
                                def tokens = line[7..-1].tokenize()
                                detalleSectionRecord.valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                tokens.pop() // Remove 'Q'
                                detalleSectionRecord.concepto = tokens.join(' ')
                                break
                            case ~/^3200600/:
                                TxtDigester.SectionRecord totalSectionRecord = new TxtDigester.SectionRecord()
                                sectionRecord.notasCredito.last().total = totalSectionRecord
                                // --
                                def tokens = line[7..-1].tokenize()
                                totalSectionRecord.valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
                                tokens.pop() // Remove 'Q'
                                totalSectionRecord.concepto = tokens.join(' ')
                                break
                            case ~/^3200610/:
                                sectionRecord.notasCredito.last().totalLetras = line[7..-1].trim().replaceAll('\\s{2,}', ' ') // Replace multiple blank spaces with a single one
                                break
                            case ~/^3200700/:
                                sectionRecord.notasCredito.last().motivo = line[7..-1]
                                break
                        }
                    } catch (Exception e) {
                        Globals.LINE_ERROR = line
                        throw e
                    }
                }
                return sectionRecord
            }
        ],
    ]
}



class TxtDigester {
    private static Map txtSections

    TxtDigester(Map txtSections/*, Map datTables*/) {
        this.txtSections = txtSections
        //this.datTables = datTables
    }

    static class MultisectionRecord extends LinkedHashMap {

        static String quantitySummationClassifiedByConsumptionType(SectionRecord servicio, TxtSpecs.ConsumoType consumptionType) {
            //BigDecimal summation = new BigDecimal('0')
            // -- VOICE:
            Integer minutes = 0
            Integer seconds = 0
            // -- ET AL:
            Integer quantity = 0

            //0900400  TOTAL: 4 MINUTOS LOCALES
            //0900500  4 MINUTOS LOCALES CON VALOR  DE Q0.00
            //0900500  0 MINUTOS LOCALES CON VALOR  DE Q0.00
            //1100400  TOTAL: 427 MINUTOS LLAMADAS A OPERADORES
            //1100400  145 MINUTOS LLAMADAS A OPERADORES CON VALOR  DE Q0.00
            //1100400  282 MINUTOS LLAMADAS A OPERADORES CON VALOR  DE Q196.74

            servicio.consumos.each { SectionRecord consumo ->
                LinkedHashSet dynamicKeys = consumo.keySet()
                dynamicKeys.forEach { String key ->
                    if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.LLAMADA) {
                        //if (consumo[key].totales.size() == 0) {}
                        consumo[key].totales.each { String total ->
                            if (total.startsWith("TOTAL")) {
                                def tokens = total.tokenize()
                                def cantidad = FormatUtil.currencyToNumberFormatter(tokens[1]).toInteger()
                                quantity += cantidad
                            }
                        }
                    } else if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.OTRO) {
                        quantity += consumo[key].detalleConsumos.size()
                    } else if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.EVENTO) {
                        consumo[key].totales.each { String total ->
                            if (total.startsWith("TOTAL")) {
                                def tokens = total.tokenize()
                                def cantidad = FormatUtil.currencyToNumberFormatter(tokens[1]).toInteger()
                                quantity += cantidad
                            }
                        }
                    }
                }
            }

            if (minutes != 0) {
                return "${minutes}:${seconds}"
            } else {
                return "${quantity}"
            }
        }

        static BigDecimal valueSummationClassifiedByConsumptionType(SectionRecord servicio, TxtSpecs.ConsumoType consumptionType) {
            BigDecimal summation = new BigDecimal('0.00')
            //summation = summation.setScale(2)
            //summation = summation.setScale(2, BigDecimal.ROUND_HALF_EVEN);

            //0900400  TOTAL: 4 MINUTOS LOCALES
            //0900500  4 MINUTOS LOCALES CON VALOR  DE Q0.00
            //0900500  0 MINUTOS LOCALES CON VALOR  DE Q0.00
            //1100400  TOTAL: 427 MINUTOS LLAMADAS A OPERADORES
            //1100400  145 MINUTOS LLAMADAS A OPERADORES CON VALOR  DE Q0.00
            //1100400  282 MINUTOS LLAMADAS A OPERADORES CON VALOR  DE Q196.74
            servicio.consumos.each { SectionRecord consumo ->
                LinkedHashSet dynamicKeys = consumo.keySet()
                dynamicKeys.forEach { String key ->
                    if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.LLAMADA) {
                        if (consumo[key].totales.size() == 1) {
                            def tokens = consumo[key].totales[0].tokenize()
                            def valor = FormatUtil.currencyToNumberFormatter(tokens[-1])
                            if (valor ==~ /\d+\.\d{2}/) { // Is numeric value?
                                summation += valor.toBigDecimal()
                            }
                        } else if (consumo[key].totales.size() > 1) {
                            consumo[key].totales.each { String total ->
                                if (!total.startsWith("TOTAL")) {
                                    def tokens = total.tokenize()
                                    def valor = FormatUtil.currencyToNumberFormatter(tokens[-1])
                                    summation += valor.toBigDecimal()
                                }
                            }
                        }
                    } else if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.OTRO) {
                        consumo[key].totales.each { String total ->

                            if (total.startsWith("TOTAL")) {
                                def tokens = total.tokenize()
                                def valor = FormatUtil.currencyToNumberFormatter(tokens[-1])
                                summation += valor.toBigDecimal()
                            }
                        }
                    } else if (consumo[key].consumoType == consumptionType && consumptionType == TxtSpecs.ConsumoType.EVENTO   ) {
                        consumo[key].totales.each { String total ->
                            if (total.startsWith("TOTAL")) {
                                def tokens = total.tokenize()
                                def valor = FormatUtil.currencyToNumberFormatter(tokens[-1])
                                summation += valor.toBigDecimal()
                            }
                        }
                    }
                }
            }

            //return summation.setScale(2, BigDecimal.ROUND_HALF_EVEN)
            //return summation.round(new java.math.MathContext(2, RoundingMode.UNNECESSARY))
            println "FINAL SUMMATION ${summation}"
            return summation
        }
    }

    static class SectionRecord extends LinkedHashMap {
        SectionRecord() {

        }

        SectionRecord(String id) {
            this.put('id', id)
        }

        SectionRecord(Map args) {
            this.put('id', args.id)
        }

        LinkedHashSet dynamicKeys() {
            return this.keySet().findAll({it.contains('_')})
        }
    }

    private static boolean populateMultilineRecord(MultisectionRecord multisectionRecord, List<String> sectionLines, TxtSpecs.SectionType sectionType) {
        try {
            SectionRecord sectionRecord = txtSections[sectionType.id].parse(sectionLines)
            multisectionRecord[sectionType.id] = sectionRecord
        } catch(Exception ex) {
            println "EXCEPTION on sectionType [${sectionType.id}] line: ${Globals.LINE_ERROR}"
            throw ex
        }

        //println "lineRecord(${lineRecord.size()}): ${lineRecord}"

        //println "***populateMultilineRecord closure took ${TimeCategory.minus(new Date(), start)}"
        return true
    }

    private static void postPopulateMultisectionRecord(MultisectionRecord multisectionRecord) {
        multisectionRecord['0100000'].categoria = TxtSpecs.CategoriaType.forCiclo(multisectionRecord['0100000'].ciclo.toInteger()).name()

        if (!multisectionRecord.containsKey('0200000')) {
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '0200000'
            sectionRecord.conceptosCobro = [] // new List
            sectionRecord.financiamientos = [] // new List
            sectionRecord.productosServicios = [] // new List
            multisectionRecord['0200000'] = sectionRecord
        }
        if (!multisectionRecord['0200000'].containsKey('totalPagar')) { // REFACTURA!!!!!
            multisectionRecord['0100000'].refactura = 'S'

            if (multisectionRecord['0100000'].personId.isEmpty()) {
                multisectionRecord['0100000'].personId = multisectionRecord['0100000'].documentoNumeroV2
            }

            /*<conceptosCobro>
                <conceptoCobro nombre="SALDO ANTERIOR" valor="233.04" periodo="JULIO/2021" />
                <conceptoCobro nombre="PAGOS EFECTUADOS (-)" valor="233.04" periodo="" />
                <conceptoCobro nombre="SALDO INICIAL" valor="0.00" periodo="AGOSTO/2021" />
                <conceptoCobro nombre="CARGOS DEL MES" valor="0" periodo="" />
            </conceptosCobro>*/
            // TODO: Check if conceptosCobro List exists
            multisectionRecord['0200000'].conceptosCobro = [] // new List
            // --
            //0200300        SALDO ANTERIOR
            TxtDigester.SectionRecord conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
            conceptoCobroSectionRecord.nombre = 'SALDO ANTERIOR'
            multisectionRecord['0200000'].conceptosCobro << conceptoCobroSectionRecord
            // --
            //0200400        SU PAGO GRACIAS
            conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
            conceptoCobroSectionRecord.nombre = 'PAGOS EFECTUADOS (-)'
            multisectionRecord['0200000'].conceptosCobro << conceptoCobroSectionRecord
            // --
            //0200500        SALDO INICIAL
            conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
            conceptoCobroSectionRecord.nombre = 'SALDO INICIAL'
            multisectionRecord['0200000'].conceptosCobro << conceptoCobroSectionRecord
            // --
            // 0200600        CARGOS DEL MES
            conceptoCobroSectionRecord = new TxtDigester.SectionRecord()
            conceptoCobroSectionRecord.nombre = 'CARGOS DEL MES'
            multisectionRecord['0200000'].conceptosCobro << conceptoCobroSectionRecord
        }
        if (!multisectionRecord.containsKey('1400100')) { // INTEGRADOS
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '1400100'
            sectionRecord.cabeceras = ''
            sectionRecord.columnas = 0
            sectionRecord.numeroLineas = 0
            sectionRecord.resumenIntegrados = [] // new List
            //sectionRecord.totales = [:]
            multisectionRecord['1400100'] = sectionRecord
        }
        if (!multisectionRecord.containsKey('1401100')) { // INTEGRADOS
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '1401100'
            sectionRecord.cabeceras = ''
            sectionRecord.columnas = 0
            sectionRecord.nombre = ''
            sectionRecord.subNombre = ''
            sectionRecord.cargos = [] // new List
            //sectionRecord.totales = [:]
            multisectionRecord['1401100'] = sectionRecord
        }
        if (!multisectionRecord.containsKey('1600100')) { // FINANCIAMIENTOS
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '1600100'
            sectionRecord.titulo = ''
            sectionRecord.cabeceras = ''
            sectionRecord.columnas = 0
            sectionRecord.totalFinanciamientoMes = 0.0
            sectionRecord.financiamientos = [] // new List
            multisectionRecord['1600100'] = sectionRecord
        }
        if (!multisectionRecord.containsKey('1800100')) { // ENLACES
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '1800100'
            sectionRecord.cabeceras = ''
            sectionRecord.columnas = 0
            sectionRecord.titulo = ''
            sectionRecord.fecha = ''
            sectionRecord.enlaces = [] // new List
            multisectionRecord['1800100'] = sectionRecord
        }
        if (!multisectionRecord.containsKey('3000000')) { // NOTAS DEBITO
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '3000000'
            sectionRecord.notasAbonado = [] // new List
            multisectionRecord['3000000'] = sectionRecord
        }
        if (!multisectionRecord.containsKey('3200000')) { // NOTAS CREDITO
            TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
            sectionRecord.id = '3200000'
            sectionRecord.notasCredito = [] // new List
            multisectionRecord['3200000'] = sectionRecord
        }
    }

    static MultisectionRecord buildMultisectionRecord(List<String> lines, String id='X') {
        if (lines) {
            println "${this.class.canonicalName}[${id}] processing List of ${lines.size()} lines on thread ${Thread.currentThread().name}" //as java.lang.Object
        }

        if (lines.isEmpty()) {
            return null
        }

        MultisectionRecord multisectionRecord = new MultisectionRecord()

        List<String> sectionLines = [] // New List

        lines.each { line ->
            String lineId = line.take(TxtSpecs.LINE_ID_LENGTH)
            boolean startsNewSection = TxtSpecs.SectionType.startsNewSectionExcludingVeryFirstAndLastSections(lineId)
            //TxtSpecs.SectionType sectionType = TxtSpecs.SectionType.forId(lineId)
            //boolean endOfMultilineSection = TxtSpecs.SectionType.endOfMultilineSection(sectionType)
            boolean endOfMultilineSection = TxtSpecs.SectionType.endOfMultilineSection(lineId)

            if (startsNewSection || endOfMultilineSection) {
                TxtSpecs.SectionType currentSectionType = TxtSpecs.SectionType.forId(sectionLines[0].take(TxtSpecs.LINE_ID_LENGTH))
                populateMultilineRecord(multisectionRecord, sectionLines, currentSectionType) // Process previous sectionLines
                sectionLines = [] // New List
            }

            sectionLines << line
        }
        // --
        postPopulateMultisectionRecord(multisectionRecord)

        return multisectionRecord
    }

    static String calcUidMD5(MultisectionRecord multisectionRecord) {
        return CryptoUtil.calculateMD5(multisectionRecord['0100000'].personId + multisectionRecord['0100000'].fechaEmision)
    }
}



class XmlUtil {

    static String buildXml(TxtDigester.MultisectionRecord multisectionRecord) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.setDoubleQuotes(true)

        // MarkupBuilder gives us an instance of MarkupBuilderHelper named 'mkp'
        // MarkupBuilderHelper has several helpful methods
//        xml.mkp.xmlDeclaration(version: "1.0", encoding: "utf-8")
        //xml.'ubf:bill'('version': 1) { // ROOT TAG
        //xml.DOCUMENT('xmlns:ubf': 'com/sorrisotech/saas/ubf/v1/UBF') { // ROOT TAG
        xml.BILL { // ROOT TAG
            'ubf:info' { // TAG
                'ubf:billId' "C${multisectionRecord['0100000'].personId}C${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_yyyyMMdd(multisectionRecord['0100000'].fechaEmision)}" // TAG
                'ubf:internalAccountNo' "C${multisectionRecord['0100000'].personId}C" // TAG
                'ubf:externalAccountNo' "${multisectionRecord['0100000'].personId}" // TAG
                'ubf:billDate' "${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(multisectionRecord['0100000'].fechaEmision)}" // TAG
                'ubf:dueDate' "${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(multisectionRecord['0100000'].fechaVencimiento)}" // TAG
                'ubf:finalBill' false // TAG
                'ubf:accountClosed' false // TAG
                'ubf:disablePayment' false // TAG
                // 'ubf:amountDue'  "${multisectionRecord['0100000'].totalPagar}" // TAG
                // 'ubf:billAmount' "${multisectionRecord['0100000'].totalPagar}" // TAG
                // 'ubf:minimumDue' "${multisectionRecord['0100000'].totalPagar}" // TAG
                // 20221220 Jasiel
                'ubf:amountDue' "${FormatUtil.currencyToNumberFormatter(multisectionRecord['0100000'].totalPagar ?: '0.00')}" // TAG
                'ubf:billAmount' "${FormatUtil.currencyToNumberFormatter(multisectionRecord['0100000'].totalPagar ?: '0.00')}" // TAG
                'ubf:minimumDue' "${FormatUtil.currencyToNumberFormatter(multisectionRecord['0100000'].totalPagar ?: '0.00')}" // TAG
				//
                'ubf:overdue' false // TAG
                'ubf:billingAddress' { // TAG
                    'ubf:line' "${multisectionRecord['0100000'].receptorDirL1}" // TAG
                    'ubf:line' "${multisectionRecord['0100000'].receptorDirL2}" // TAG
                    'ubf:line' "${multisectionRecord['0100000'].receptorDirL3}" // TAG
                }
                'ubf:orgId' 'ORG_ID'
                'ubf:costCenter' 'COST_CENTER'
                'ubf:selfReg0' "${multisectionRecord['0100000'].facturaSerie} ${multisectionRecord['0100000'].documentoNumeroV2}" // TAG
                'ubf:selfReg1' 'F-' //"${multisectionRecord['01000'].nitReceptor}" // TAG
                'ubf:selfReg2' ' ' // TAG
                //'ubf:selfReg3' "BASE_URL/c_gtfija/embed-iframe.php?uid=${CryptoUtil.calculateMD5(multisectionRecord['0100000'].personId + multisectionRecord['0100000'].fechaEmision)}" // TAG
                'ubf:selfReg3' "BASE_URL/c_gtfija/embed-iframe.php?uid=${TxtDigester.calcUidMD5(multisectionRecord)}" // TAG
                'ubf:selfReg4' "${multisectionRecord['0100000'].documentoNumeroV2}" // TAG
            }
            'ubf:summary-xml'(layout: 'telco.demo', version: 1) { // TAG
                emisor { // TAG
                    nombre "${multisectionRecord['0100000'].emisorNombre}" // TAG
                    nit "${multisectionRecord['0100000'].emisorNIT}" // TAG
                    direccionL1 "${multisectionRecord['0100000'].emisorDirL1}" // TAG
                    direccionL2 "${multisectionRecord['0100000'].emisorDirL2}" // TAG
                    direccionL3 "${multisectionRecord['0100000'].emisorDirL3}" // TAG
                } // emisor
                documentoTributarioElectronico { // TAG
                    facturaSerie "${multisectionRecord['0100000'].facturaSerie}" // TAG
                    documentoNumero "${multisectionRecord['0100000'].documentoNumeroV2}" // TAG
                    //ciclo "${multisectionRecord['0100000'].ciclo}" // TAG
                    //correlativo "${multisectionRecord['0100000'].correlativo}" // TAG
                    //rutaCourier "${multisectionRecord['0100000'].rutaCourier}" // TAG
                    numeroAutorizacion "${multisectionRecord['0100000'].numeroAutorizacion}" // TAG
                    consultas "${multisectionRecord['0100000'].consultas}"
                } // documentoTributarioElectronico
                receptor { // TAG
                    nombre "${multisectionRecord['0100000'].receptorNombre}" // TAG
                    nit "${multisectionRecord['0100000'].receptorNIT}" // TAG
                    direccionL1 "${multisectionRecord['0100000'].receptorDirL1}" // TAG
                    direccionL2 "${multisectionRecord['0100000'].receptorDirL2 ?: ''}" // TAG
                    direccionL3 "${multisectionRecord['0100000'].receptorDirL3}" // TAG
                    telefono "${multisectionRecord['0100000'].receptorTelefono}" // TAG
                    personId "${multisectionRecord['0100000'].personId}" // TAG
                    correo "${multisectionRecord['0100000'].eMails}" // TAG
                } // receptor
                estadoCuenta { // TAG
                    numero "${multisectionRecord['0100000'].documentoNumero}" // TAG
                    ciclo "${multisectionRecord['0100000'].ciclo}" // TAG
                    correlativo "${multisectionRecord['0100000'].correlativo}" // TAG
                    rutaCourier "${multisectionRecord['0100000'].rutaCourier}" // TAG
                    fechaEmision "${multisectionRecord['0100000'].fechaEmision}" // TAG
                    imprimeCiclo "${multisectionRecord['0100000'].imprimeCiclo}" // TAG
                    conFinanciamiento "${multisectionRecord['0100000'].conFinanciamiento}" // TAG
                    tipoFactura "${multisectionRecord['0100000'].tipoFactura}" // TAG
                    imprimeBPS "${multisectionRecord['0100000'].imprimeBPS}" // TAG
                    refactura "${multisectionRecord['0100000'].refactura}" // TAG
                    segmentacion "${multisectionRecord['0100000'].segmentacion}" // TAG
                    periodoFacturado "${multisectionRecord['0200000'].periodoFacturado}" // TAG
                    servicio "${multisectionRecord['0100000'].servicio}" // TAG
                    limite "${multisectionRecord['0100000'].limite ?: ''}" // TAG
                    mensaje "${multisectionRecord['0100000'].mensaje}" // TAG
                    fechaVencimiento "${multisectionRecord['0100000'].fechaVencimiento}" // TAG
                    conceptosCobro { // TAG
                        multisectionRecord['0200000'].conceptosCobro.each { TxtDigester.SectionRecord item ->
                            conceptoCobro( // TAG
                                nombre: item.nombre,
                                valor: item.valor ?: 0.0,
                                periodo: item.periodo ?: ''
                            )
                        }
                    } // conceptosCobro
                    financiamientos { // TAG
                        multisectionRecord['0200000'].financiamientos.each { TxtDigester.SectionRecord item ->
                            financiamiento( // TAG
                                nombre: item.nombre,
                                valor: item.valor ?: 0,
                                periodo: item.periodo
                            )
                        }
                    } // financiamientos
                    productosServicios { // TAG
                        multisectionRecord['0200000'].productosServicios.each { TxtDigester.SectionRecord item ->
                            productoServicio(nombre: item.nombre, valor: item.valor, periodo: item.periodo) // TAG
                        }
                    } // productosServicios
                    totalFactura "${multisectionRecord['0200000'].totalFactura}" // TAG
                    aviso "${multisectionRecord['0200000'].aviso}" // TAG
                    totalPagar "${multisectionRecord['0200000'].totalPagar ?: '0.00'}" // TAG
                    notificaciones { // TAG
                        multisectionRecord['0200000'].notificaciones.each {
                            notificacion "${it}" // TAG
                        }
                    } // notificaciones
                } // estadoCuenta
                resumen { // TAG
                    serieAdministrativa "${multisectionRecord['0400200'].serieAdministrativa}"
                    numeroAdministrativo "${multisectionRecord['0400200'].numeroAdministrativo}"
                    numeroAutorizacion "${multisectionRecord['0400200'].numeroAutorizacion}" // TAG
                    facturaSerie "${multisectionRecord['0400200'].facturaSerie}" // TAG
                    clienteNombre "${multisectionRecord['0400200'].clienteNombre}" // TAG
                    telefono "${multisectionRecord['0400200'].telefono}" // TAG
                    mesFacturacion "${multisectionRecord['0400200'].mesFacturacion}" // TAG
                    totalMes "${multisectionRecord['0400200'].totalMes}" // TAG
                    if (multisectionRecord['0400200'].containsKey('totalPagar')) {
                        totalPagar "${multisectionRecord['0400200'].totalPagar}" // TAG
                        totalPagarLetras "${FormatUtil.numberToLetters(multisectionRecord['0400200'].totalPagar)}" // TAG
                    } else {
                        totalPagar "${multisectionRecord['0400200'].totalMes}" // TAG
                        totalPagarLetras "${FormatUtil.numberToLetters(multisectionRecord['0400200'].totalMes)}" // TAG
                    }
                    vencimiento "${multisectionRecord['0400200'].vencimiento ?: ''}" // TAG
                    certificador "${multisectionRecord['0400200'].certificador}" // TAG
                    certificadorNIT "${multisectionRecord['0400200'].certificadorNIT}" // TAG
                    simboloMoneda 'Q' // TAG

                    ajuste "${multisectionRecord['0100000'].ajuste}" // TAG // TODO: Pendiente*****
                    financimiento( // TAG
                        multisectionRecord['1600100'].financiamientos.flatten().productos.flatten().productList.flatten().findAll {
                            it.id == '1600300' && !it.producto.contains('Convenio')
                        }.collect {
                            it.valorCuota.toBigDecimal()
                        }.sum(0.00)
                    )
                    convenio( // TAG
                        multisectionRecord['1600100'].financiamientos.flatten().productos.flatten().productList.flatten().findAll {
                            it.id == '1600300' && it.producto.contains('Convenio')
                        }.collect{
                            it.valorCuota.toBigDecimal()
                        }.sum(0.00)
                    )

                } // resumen
                resumenIntegrados(cabeceras: multisectionRecord['1400100'].cabeceras.join(', '), columnas: multisectionRecord['1400100'].columnas, numeroLineas: multisectionRecord['1400100'].numeroLineas) { // TAG
                    multisectionRecord['1400100'].resumenIntegrados.each { TxtDigester.SectionRecord item ->
                        integrado( // TAG
                                telefono: item.telefono,
                                cable: item.cable,
                                turbonet: item.turbonet,
                                cuota: item.cuota,
                                minLocal: item.minLocal,
                                dia: item.dia,
                                dna: item.dna,
                                minOper: item.minOper,
                                otros: item.otros,
                                total: item.total
                        )
                    }
                    if (multisectionRecord.containsKey('1400100') && multisectionRecord['1400100'].containsKey('totales')) {
                        totales( // TAG
                            descripcion: multisectionRecord['1400100'].totales.telefono,
                            cable: multisectionRecord['1400100'].totales.cable,
                            turbonet: multisectionRecord['1400100'].totales.turbonet,
                            cuota: multisectionRecord['1400100'].totales.cuota,
                            minLocal: multisectionRecord['1400100'].totales.minLocal,
                            dia: multisectionRecord['1400100'].totales.dia,
                            dna: multisectionRecord['1400100'].totales.dna,
                            minOper: multisectionRecord['1400100'].totales.minOper,
                            otros: multisectionRecord['1400100'].totales.otros,
                            total: multisectionRecord['1400100'].totales.total
                        )
                    }
                } // resumenIntegrados
                cargos( // TAG
                    cabeceras: multisectionRecord['1401100'].cabeceras.join(', '),
                    columnas: multisectionRecord['1401100'].columnas,
                    nombre: multisectionRecord['1401100'].nombre,
                    subNombre: multisectionRecord['1401100'].subNombre)
                {
                    multisectionRecord['1401100'].cargos.each { TxtDigester.SectionRecord item ->
                        println "MINUTOS CURSADOS: ${item.minutosCursados}"
                        cargo( // TAG
                            telefono: item.telefono,
                            minutosCursados: item.minutosCursados,
                            periodo: item.periodo,
                            minutosSinCosto: item.minutosSinCosto,
                            porPagar: item.porPagar,
                            costoUnitario: item.costoUnitario,
                            total: item.total
                        )
                    }
                    if (multisectionRecord.containsKey('1401100') && multisectionRecord['1401100'].containsKey('totales')) {
                        totales( // TAG
                            descripcion: multisectionRecord['1401100'].totales.descripcion,
                            total: multisectionRecord['1401100'].totales.total,
                            porPagar: multisectionRecord['1401100'].totales.porPagar,
                            minutosSinCosto: multisectionRecord['1401100'].totales.minutosSinCosto,
                            minutosCursados: multisectionRecord['1401100'].totales.minutosCursados
                        )
                    }
                } // cargos
                enlaces( // TAG
                        cabeceras: multisectionRecord['1800100'].cabeceras.join(', '),
                        columnas: multisectionRecord['1800100'].columnas,
                        titulo: multisectionRecord['1800100'].titulo,
                        fecha: multisectionRecord['1800100'].fecha
                ) {
                    multisectionRecord['1800100'].enlaces.each { TxtDigester.SectionRecord item ->
                        enlace( // TAG
                            codigoEnlace: item.codigoEnlace,
                            codigoServ: item.codigoServ,
                            descripcionServicio: item.descripcionServicio,
                            direccionCompleta: item.direccionCompleta,
                            cargo: item.cargo,
                            valorS: item.valorS,
                            valorQ: item.valorQ
                        )
                    }
                } // enlaces
                financiamientos( // TAG
                        cabeceras: multisectionRecord['1600100'].cabeceras.join(', '),
                        columnas: multisectionRecord['1600100'].columnas,
                        titulo: multisectionRecord['1600100'].titulo,
                        totalFinanciamientoMes: multisectionRecord['1600100'].totalFinanciamientoMes
                ) {
                    financiamiento { // TAG
                        multisectionRecord['1600100'].financiamientos.each { TxtDigester.SectionRecord financing ->
                            financing.productos.each { TxtDigester.SectionRecord item ->
                                productos { // TAG
                                    item.productList.each { TxtDigester.SectionRecord product ->
                                        if (product.id == '1600300') {
                                            producto( // TAG
                                                nombre: product.producto,
                                                serie: product.serie,
                                                financiamiento: product.financiamiento,
                                                saldo: product.saldo,
                                                valorCuota: product.valorCuota,
                                                numCuota: product.numCuota
                                            )
                                        } else if (product.id == '1600400') {
                                            total( // TAG
                                                concepto: product.concepto,
                                                financiamiento: product.financiamiento,
                                                saldo: product.saldo,
                                                valorCuota: product.valorCuota
                                            )
                                        }
                                    }
                                } // productos
                            }
                        }
                    } // financiamiento
                } // financiamientos
                notasAbonado { // TAG
                    multisectionRecord['3000000'].notasAbonado.each { TxtDigester.SectionRecord item ->
                        notaAbonado { // TAG
                            nota {  // TAG
                                serie "${item.nota.serie}"
                                numero "${item.nota.numero}" // TAG
                                numeroAutorizacion "${item.nota.numeroAutorizacion}" // TAG
                                fechaEmision "${item.nota.fechaEmision}" // TAG
                            } // nota
                            cliente { // TAG
                                nombre "${item.cliente.nombre}" // TAG
                                direccion "${item.cliente.direccion}" // TAG
                                nit "${item.cliente.nit}" // TAG
                                datoX "${item.cliente.datoX}" // TAG
                                datoY "${item.cliente.datoY}" // TAG
                                telefono "${item.cliente.telefono}" // TAG
                            } // cliente
                            facturaAsociada { // TAG
                                fecha "${item.facturaAsociada.fecha}" // TAG
                                serie "${item.facturaAsociada.serie}" // TAG
                                numero "${item.facturaAsociada.numero}" // TAG
                                telefono "${item.facturaAsociada.telefono}" // TAG
                                monto "${item.facturaAsociada.monto}" // TAG
                            } // facturaAsociada
                            certificador { // TAG
                                nombre "${item.certificador.nombre}" // TAG
                                nit "${item.certificador.nit}" // TAG
                                serieAdministrativa "${item.certificador.serieAdministrativa}" // TAG
                                numeroAdministrativo "${item.certificador.numeroAdministrativo}" // TAG
                            } // certificador
                            detalles { // TAG
                                item.detalles.each {
                                    detalle(concepto: it.concepto, valor: it.valor) // TAG
                                }
                                total(concepto: item.total.concepto, valor: item.total.valor) // TAG
                                totalLetras "${item.totalLetras}" // TAG
                            } // detalles
                            motivo "${item.motivo}"
                        } // notaAbonado
                    }
                } // notasAbonado
                notasCredito { // TAG
                    multisectionRecord['3200000'].notasCredito.each { TxtDigester.SectionRecord item ->
                        notaCredito { // TAG
                            nota {  // TAG
                                serie "${item.nota.serie}"
                                numero "${item.nota.numero}" // TAG
                                numeroAutorizacion "${item.nota.numeroAutorizacion}" // TAG
                                fechaEmision "${item.nota.fechaEmision}" // TAG
                            } // nota
                            cliente { // TAG
                                nombre "${item.cliente.nombre}" // TAG
                                direccion "${item.cliente.direccion}" // TAG
                                nit "${item.cliente.nit}" // TAG
                                datoX "${item.cliente.datoX}" // TAG
                                datoY "${item.cliente.datoY}" // TAG
                                telefono "${item.cliente.telefono}" // TAG
                            } // cliente
                            facturaAsociada { // TAG
                                fecha "${item.facturaAsociada.fecha}" // TAG
                                serie "${item.facturaAsociada.serie}" // TAG
                                numero "${item.facturaAsociada.numero}" // TAG
                                telefono "${item.facturaAsociada.telefono}" // TAG
                                monto "${item.facturaAsociada.monto}" // TAG
                            } // facturaAsociada
                            certificador { // TAG
                                nombre "${item.certificador.nombre}" // TAG
                                nit "${item.certificador.nit}" // TAG
                                serieAdministrativa "${item.certificador.serieAdministrativa}" // TAG
                                numeroAdministrativo "${item.certificador.numeroAdministrativo}" // TAG
                            } // certificador
                            detalles { // TAG
                                item.detalles.each {
                                    detalle(concepto: it.concepto, valor: it.valor) // TAG
                                }
                                total(concepto: item.total.concepto, valor: item.total.valor) // TAG
                                totalLetras "${item.totalLetras}" // TAG
                            } // detalles
                            motivo "${item.motivo}"
                        } // notaCredito
                    }
                } // notasCredito
                phoneUsage { // TAG
                    multisectionRecord['0700100'].servicios.each { TxtDigester.SectionRecord servicioItem ->
                        //List llamadas = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                        String sumatoriaLlamadasCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.LLAMADA)
                        BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.LLAMADA)
                        // --
                        //List mensajes = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                        String sumatoriaOtrosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.OTRO)
                        BigDecimal sumatoriaOtrosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.OTRO)
                        // --
                        //List datos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        //String sumatoriaDatosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        //BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        // --
                        //List eventos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                        String sumatoriaEventosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.EVENTO)
                        BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.EVENTO)
                        // --
                        //BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaOtrosValor + sumatoriaEventosValor
                        // --
                        phone(target: "${servicioItem.telefonoV2}.phone") { // TAG
                            voice(minutes: sumatoriaLlamadasCantidad, cost: sumatoriaLlamadasValor, "${servicioItem.telefonoV2}.calls") // TAG
                            data(usage: sumatoriaEventosCantidad, cost: sumatoriaEventosValor, "${servicioItem.telefonoV2}.data") // TAG
                            text(count: sumatoriaOtrosCantidad, cost: sumatoriaOtrosValor, "${servicioItem.telefonoV2}.text") // TAG
                        } // phone
                    }
                } // phoneUsage
                phones { // TAG
                    multisectionRecord['0700100'].servicios.each { TxtDigester.SectionRecord servicioItem ->
//                        def nombre
//                        def detalleServicio = multisectionRecord['200300'].detalleServiciosAgrupados.find({it.telefono == servicioItem.telefono.replace('-', '')})
//                        if (detalleServicio != null) {
//                            nombre = detalleServicio.razonSocial
//                        } else {
//                            nombre = multisectionRecord['100200'].clienteNombre
//                        }
                        phone(id: servicioItem.telefonoV2, target: "${servicioItem.telefonoV2}.phone") { // TAG
                            summary { // TAG
                                name(multisectionRecord['0100000'].receptorNombre) // TAG
                                number(servicioItem.telefonoV2)
                            } // summary
                        } // phone
                    }
                } // phones
            } // ubf:summary-xml
            // *****************************************************************************************************
            'ubf:children' {} // Empty TAG
        } // BILL

        return writer.toString()
    }

    static String buildChildrenDoc1(TxtDigester.MultisectionRecord multisectionRecord) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.setDoubleQuotes(true)

        xml.'ubf:children' { // TAG
            multisectionRecord['0700100'].servicios.each { TxtDigester.SectionRecord servicioItem ->
//                    List llamadas = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
//                    String sumatoriaLlamadasCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.LLAMADA)
//                    // --
//                    List mensajes = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    String sumatoriaMensajesCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    BigDecimal sumatoriaMensajesValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    // --
//                    List datos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    String sumatoriaDatosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    // --
//                    List eventos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    String sumatoriaEventosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    // --
//                    BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaMensajesValor + sumatoriaDatosValor
//                    // --
                'ubf:child'( // TAG
                        id: servicioItem.telefono,
                        linkid: "${servicioItem.telefono}.phone"
                ) {
                    'ubf:info' { // TAG
                        'ubf:name' multisectionRecord['0100000'].receptorNombre // TAG
                        'ubf:totalCharges' '0.00' //"${sumatoriaLlamadasMensajes}" // TAG
                    } // ubf:info
                    'ubf:display' { // TAG
                        'ubf:language' 'ES' // TAG
                        'ubf:layout' 'telco.phone.demo' // TAG
                    } // ubf:display
                    'ubf:currency' { // TAG
                        'ubf:code' 'QTZ' // TAG
                        'ubf:symbol' 'Q' // TAG
                    } // ubf:currency
                    'ubf:summary-xml' { // TAG
                        monthly(total: '0.00') { // TAG
                            planFamilyPlan(label: 'Family plan', '0.00') // TAG
                            unlimitedData(label: 'Unlimited data', '0.00') // TAG
                        } // monthly
                        usageSummary(total: "${FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)}") { // TAG
                            voiceCalls(label: 'Llamadas de Voz', FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)) // TAG
                            //data(label: 'Uso de Datos', FormatUtil.NumberFormat_2dec.format(sumatoriaDatosValor)) // TAG
                            //text(label: 'Mensajes de Texto', FormatUtil.NumberFormat_2dec.format(sumatoriaMensajesValor)) // TAG
                        }
                        rebate(total: '0.00') { // TAG
                            valueRebate(label: 'Reembolso para el Cliente', '0.00') // TAG
                        }
                        taxes(total: '0.00') { // TAG
                            monthly '0.00' // TAG
                            usage '0.00' // TAG
                        }
                    } // ubf:summary-xml
                    'ubf:data' { // TAG
                        'ubf:dataGroup'(id: servicioItem.telefonoV2, linkId: servicioItem.telefonoV2, layout: 'basic.call') { // TAG
                            'ubf:name'(type: 'id', 'Consumos') // TAG
                            'ubf:display' { // TAG
                                'ubf:language' 'ES' // TAG
                                'ubf:layout' 'telco' // TAG
                            } // ubf:display
                            'ubf:currency' { // TAG
                                'ubf:code' 'QTZ' // TAG
                                'ubf:symbol' 'Q' // TAG
                            } // ubf:currency
                            'ubf:rawContents' { // TAG
                                servicioItem.consumos.each { TxtDigester.SectionRecord consumoItem ->
                                    LinkedHashSet consumptionKeys = consumoItem.dynamicKeys()
                                    consumptionKeys.each { String consumptionKey ->
                                        def consumoNombre = consumptionKey.tokenize('_').join(' ').toUpperCase()
                                        consumos(nombre: consumoNombre) { // TAG
                                            "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}s"( // TAG
                                                    cabeceras: consumoItem[consumptionKey].cabeceras.join(', '),
                                                    columnas: consumoItem[consumptionKey].columnas
                                            ) {
                                                consumoItem[consumptionKey].detalleConsumos.each { TxtDigester.SectionRecord item ->
                                                    if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.LLAMADA) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                destino: item.destino,
                                                                lugar: "${item.lugar ?: ''}",
                                                                min: "${item.min}",
                                                                valor: item.valor
                                                        ) // llamada
                                                    } else if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.EVENTO) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                fecha: item.fecha,
                                                                hora: item.hora,
                                                                descripcion: item.descripcion,
                                                                tarifa: item.tarifa,
                                                                valor: item.valor
                                                        ) // evento
                                                    } else if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.OTRO) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                telefono: item.telefono, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                descripcion: item.descripcion,
                                                                valor: item.valor
                                                        ) // otro
                                                    }
                                                }
                                                totales { // TAG
                                                    consumoItem[consumptionKey].totales.each {
                                                        total "${it}" // TAG
                                                    }
                                                } // totales
                                            } // llamadas|otros
                                        } // consumo
                                    }
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                    } // ubf:data
                } // ubf:child
            }
        } // ubf:children

        return writer.toString()
    }

    static String buildChildrenSmartbill(TxtDigester.MultisectionRecord multisectionRecord) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.setDoubleQuotes(true)

        xml.'ubf:children' { // TAG
            multisectionRecord['0700100'].servicios.each { TxtDigester.SectionRecord servicioItem ->
//                    List llamadas = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
//                    String sumatoriaLlamadasCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(servicioItem, TxtSpecs.ConsumoType.LLAMADA)
//                    // --
//                    List mensajes = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    String sumatoriaMensajesCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    BigDecimal sumatoriaMensajesValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    // --
//                    List datos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    String sumatoriaDatosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    // --
//                    List eventos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    String sumatoriaEventosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    // --
//                    BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaMensajesValor + sumatoriaDatosValor
//                    // --
                'ubf:child'( // TAG
                        id: servicioItem.telefonoV2,
                        linkid: "${servicioItem.telefonoV2}.phone"
                ) {
                    'ubf:info' { // TAG
                        'ubf:name' multisectionRecord['0100000'].receptorNombre // TAG
                        'ubf:totalCharges' '0.00' //"${sumatoriaLlamadasMensajes}" // TAG
                    } // ubf:info
                    'ubf:display' { // TAG
                        'ubf:language' 'ES' // TAG
                        'ubf:layout' 'telco.phone.demo' // TAG
                    } // ubf:display
                    'ubf:currency' { // TAG
                        'ubf:code' 'QTZ' // TAG
                        'ubf:symbol' 'Q' // TAG
                    } // ubf:currency
                    'ubf:summary-xml' { // TAG
                        monthly(total: '0.00') { // TAG
                            planFamilyPlan(label: 'Family plan', '0.00') // TAG
                            unlimitedData(label: 'Unlimited data', '0.00') // TAG
                        } // monthly
                        usageSummary(total: "${FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)}") { // TAG
                            voiceCalls(label: 'Llamadas de Voz', FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)) // TAG
                            //data(label: 'Uso de Datos', FormatUtil.NumberFormat_2dec.format(sumatoriaDatosValor)) // TAG
                            //text(label: 'Mensajes de Texto', FormatUtil.NumberFormat_2dec.format(sumatoriaMensajesValor)) // TAG
                        }
                        rebate(total: '0.00') { // TAG
                            valueRebate(label: 'Reembolso para el Cliente', '0.00') // TAG
                        }
                        taxes(total: '0.00') { // TAG
                            monthly '0.00' // TAG
                            usage '0.00' // TAG
                        }
                    } // ubf:summary-xml
                    'ubf:data' { // TAG
                        // ** [llamadas] ** //
                        'ubf:dataGroup'(id: "${servicioItem.telefonoV2}_calls", linkId: "${servicioItem.telefonoV2}_calls", layout: 'basic.call') { // TAG
                            'ubf:name'(type: 'id', 'Consumos') // TAG
                            'ubf:display' { // TAG
                                'ubf:language' 'ES' // TAG
                                'ubf:layout' 'telco' // TAG
                            } // ubf:display
                            'ubf:currency' { // TAG
                                'ubf:code' 'QTZ' // TAG
                                'ubf:symbol' 'Q' // TAG
                            } // ubf:currency
                            'ubf:rawContents' { // TAG
                                servicioItem.consumos.each { TxtDigester.SectionRecord consumoItem ->
                                    LinkedHashSet consumptionKeys = consumoItem.dynamicKeys()
                                    consumptionKeys.each { String consumptionKey ->
                                        if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.LLAMADA) {
                                            def consumoNombre = consumptionKey.tokenize('_').join(' ').toUpperCase()
                                            consumos(nombre: consumoNombre) { // TAG
                                                "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}s"( // TAG
                                                        cabeceras: consumoItem[consumptionKey].cabeceras.join(', '),
                                                        columnas: consumoItem[consumptionKey].columnas
                                                ) {
                                                    consumoItem[consumptionKey].detalleConsumos.each { TxtDigester.SectionRecord item ->
                                                        //if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.LLAMADA) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                destino: item.destino,
                                                                lugar: "${item.lugar ?: ''}",
                                                                min: "${item.min}",
                                                                valor: item.valor
                                                        ) // llamada
                                                        //}
                                                    }
                                                    totales { // TAG
                                                        consumoItem[consumptionKey].totales.each {
                                                            total "${it}" // TAG
                                                        }
                                                    } // totales
                                                } // llamadas|otros
                                            } // consumo
                                        }
                                    }
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                        // ** [mensajes|otros] ** //
                        'ubf:dataGroup'(id: "${servicioItem.telefonoV2}_text", linkId: "${servicioItem.telefonoV2}_text", layout: 'basic.call') { // TAG
                            'ubf:name'(type: 'id', 'Consumos') // TAG
                            'ubf:display' { // TAG
                                'ubf:language' 'ES' // TAG
                                'ubf:layout' 'telco' // TAG
                            } // ubf:display
                            'ubf:currency' { // TAG
                                'ubf:code' 'QTZ' // TAG
                                'ubf:symbol' 'Q' // TAG
                            } // ubf:currency
                            'ubf:rawContents' { // TAG
                                servicioItem.consumos.each { TxtDigester.SectionRecord consumoItem ->
                                    LinkedHashSet consumptionKeys = consumoItem.dynamicKeys()
                                    consumptionKeys.each { String consumptionKey ->
                                        def consumoNombre = consumptionKey.tokenize('_').join(' ').toUpperCase()
                                        if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.OTRO) {
                                            consumos(nombre: consumoNombre) { // TAG
                                                "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}s"( // TAG
                                                        cabeceras: consumoItem[consumptionKey].cabeceras.join(', '),
                                                        columnas: consumoItem[consumptionKey].columnas
                                                ) {
                                                    consumoItem[consumptionKey].detalleConsumos.each { TxtDigester.SectionRecord item ->
                                                        //if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.OTRO) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                telefono: item.telefono, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                descripcion: item.descripcion,
                                                                valor: item.valor
                                                        ) // otro
                                                        //}
                                                    }
                                                    totales { // TAG
                                                        consumoItem[consumptionKey].totales.each {
                                                            total "${it}" // TAG
                                                        }
                                                    } // totales
                                                } // llamadas|otros
                                            } // consumo
                                        }
                                    }
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                        // ** [datos|eventos] ** //
                        'ubf:dataGroup'(id: "${servicioItem.telefonoV2}_data", linkId: "${servicioItem.telefonoV2}_data", layout: 'basic.call') { // TAG
                            'ubf:name'(type: 'id', 'Consumos') // TAG
                            'ubf:display' { // TAG
                                'ubf:language' 'ES' // TAG
                                'ubf:layout' 'telco' // TAG
                            } // ubf:display
                            'ubf:currency' { // TAG
                                'ubf:code' 'QTZ' // TAG
                                'ubf:symbol' 'Q' // TAG
                            } // ubf:currency
                            'ubf:rawContents' { // TAG
                                servicioItem.consumos.each { TxtDigester.SectionRecord consumoItem ->
                                    LinkedHashSet consumptionKeys = consumoItem.dynamicKeys()
                                    consumptionKeys.each { String consumptionKey ->
                                        if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.EVENTO) {
                                            def consumoNombre = consumptionKey.tokenize('_').join(' ').toUpperCase()
                                            consumos(nombre: consumoNombre) { // TAG
                                                "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}s"( // TAG
                                                        cabeceras: consumoItem[consumptionKey].cabeceras.join(', '),
                                                        columnas: consumoItem[consumptionKey].columnas
                                                ) {
                                                    consumoItem[consumptionKey].detalleConsumos.each { TxtDigester.SectionRecord item ->
                                                        //if (consumoItem[consumptionKey].consumoType == TxtSpecs.ConsumoType.EVENTO) {
                                                        "${consumoItem[consumptionKey].consumoType.name().toLowerCase()}"( // TAG
                                                                fecha: item.fecha,
                                                                hora: item.hora,
                                                                descripcion: item.descripcion,
                                                                tarifa: item.tarifa,
                                                                valor: item.valor
                                                        ) // evento
                                                        //}
                                                    }
                                                    totales { // TAG
                                                        consumoItem[consumptionKey].totales.each {
                                                            total "${it}" // TAG
                                                        }
                                                    } // totales
                                                }
                                            } // llamadas|otros
                                        } // consumo
                                    }
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                    } // ubf:data
                } // ubf:child
            }
        } // ubf:children

        return writer.toString()
    }

    @Synchronized
    private static createDirectories(Path path) {
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent()) // Make sure the directories exist
        }
    }

    static void writeXmlToFile(Path filePath, String xml) {
        createDirectories(filePath)
        filePath.withWriter('UTF-8') { writer ->
            writer << xml
        }
    }

    static void generateDoc1File(Path absDirPath) {
        println 'Generating UBF.XML for Doc1'

        if (Files.exists(absDirPath)) {
            String syntaxAndPattern = 'glob:**/*.xml'
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher(syntaxAndPattern)
            def files = Files.list(absDirPath).filter({ matcher.matches(it) }).collect(Collectors.toList())
            if (files.size() == 0) {
                println "There's no XML files to generate a UBF.XML for Doc1"
                return
            }
        } else {
            println "There's no DIR to generate a UBF.XML for Doc1"
            return
        }

        Path ubfFilePath = absDirPath.getParent().resolve("ubf").resolve("b2b_${absDirPath.fileName.toString()}.doc1.xml")
        if (!Files.exists(ubfFilePath.getParent())) {
            Files.createDirectories(ubfFilePath.getParent()) // Make sure the directories exist
        }
        if (Files.exists(ubfFilePath)) {
            println "Deleting existing file ${ubfFilePath.toString()}"
            Files.delete(ubfFilePath)
        }

        ubfFilePath << '<?xml version="1.0" encoding="UTF-8"?>\n'
        ubfFilePath << '<SPOOL xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="doc1.xsd">\n'

        absDirPath.eachFileMatch(FileType.FILES, ~/.*\.xml$/) { filePath ->
            println "Merging ${filePath.fileName}"
            filePath.withReader { reader ->
                reader.eachLine { line ->
                    ubfFilePath.withWriterAppend { writer ->
                        if (line.contains('<BILL>')) {
                            writer << '<DOCUMENT xmlns:ubf="com/sorrisotech/saas/ubf/v1/UBF">\n'
                        } else if (line.contains('</BILL>')) {
                            writer << '</DOCUMENT>\n'
                        } else {
                            writer << "${line}\n"
                        }
                    }
                }
            }
        }

        ubfFilePath << '</SPOOL>'

        println "UBF.XML file ${ubfFilePath} generated for Smartbill"
    }

    static void generateDoc1FileViaGO(Path absDirPath) {
        println 'Generating UBF.XML for Doc1 via GO'
        List<String> command = ['go', 'run', '--work', 'generate_doc1_file.go', absDirPath.toString()]
        ProcessUtil.doProcess(command)
    }

    static void generateSmartbillFile(Path absDirPath) {
        println 'Generating UBF.XML for SmartBill'

        if (Files.exists(absDirPath)) {
            String syntaxAndPattern = 'glob:**/*.xml'
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher(syntaxAndPattern)
            def files = Files.list(absDirPath).filter({ matcher.matches(it) }).collect(Collectors.toList())
            if (files.size() == 0) {
                println "There's no XML files to generate a UBF.XML for SmartBill"
                return
            }
        } else {
            println "There's no DIR to generate a UBF.XML for SmartBill"
            return
        }

        Path ubfFilePath = absDirPath.getParent().resolve("ubf").resolve("b2b_${absDirPath.fileName.toString()}.ubf.xml")
        if (!Files.exists(ubfFilePath.getParent())) {
            Files.createDirectories(ubfFilePath.getParent()) // Make sure the directories exist
        }
        if (Files.exists(ubfFilePath)) {
            println "Deleting existing file ${ubfFilePath.toString()}"
            Files.delete(ubfFilePath)
        }

        ubfFilePath << '<?xml version="1.0" encoding="UTF-8"?>\n'
        ubfFilePath << '<ubf:datafile xmlns:ubf="com/sorrisotech/saas/ubf/v1/UBF">\n'
        ubfFilePath << '    <ubf:FileHeader>\n'
        ubfFilePath << '        <ubf:billStream>B2C_VIEW</ubf:billStream>\n'
        ubfFilePath << '        <ubf:paymentGroup>noop</ubf:paymentGroup>\n'
        ubfFilePath << '    </ubf:FileHeader>\n'

        absDirPath.eachFileMatch(FileType.FILES, ~/.*\.xml$/) { filePath ->
            println "Merging ${filePath.fileName}"
            filePath.withReader { reader ->
                reader.eachLine { line ->
                    ubfFilePath.withWriterAppend { writer ->
                        if (line.contains('<BILL>')) {
                            writer << '<ubf:bill version="1">\n'
//                        } else if (line.contains('<ubf:children />')) {
//                            // Reads and Writes content of document-children.smartbill
//                            String childrenFilename = filePath.getFileName().toString().replaceFirst('[.][^.]+$', '')
//                            childrenFilename = "${childrenFilename}-children.smartbill"
//                            Path childrenFilePath = absDirPath.resolve(childrenFilename)
//                            childrenFilePath.withReader { childrenReader ->
//                                childrenReader.eachLine { childrenLine ->
//                                    writer << "${childrenLine}\n"
//                                }
//                            }
                        } else if (line.contains('</BILL>')) {
                            writer << '</ubf:bill>\n'
                        } else {
                            writer << "${line}\n"
                        }
                    }
                }
            }
        }

        ubfFilePath << '    <ubf:footer>\n'
        ubfFilePath << '        <ubf:bills>100</ubf:bills>\n'
        ubfFilePath << '        <ubf:amount>63866.63</ubf:amount>\n'
        ubfFilePath << '        <ubf:assets>100</ubf:assets>\n'
        ubfFilePath << '    </ubf:footer>\n'
        ubfFilePath << '</ubf:datafile>'

        println "UBF.XML file ${ubfFilePath} generated for SmartBill"
    }

    static void generateSmartbillFileViaGO(Path absDirPath) {
        println 'Generating UBF.XML for Smartbill via GO'
        List<String> command = ['go', 'run', '--work', 'generate_smartbill_file.go', absDirPath.toString()]
        ProcessUtil.doProcess(command)
    }
}



class CsvUtil {

    enum CsvHeaderType {
        // -- Video -- //
        RECORD("RECORD"),
        UID('uid'),
        NAME('name'),
        DIAS_ACTUAL('diasActual'),
        DIAS_ANTERIOR('diasAnterior'),
        DIA_INICIO('diaInicio'),
        DIA_FINALIZA('diaFinaliza'),
        CICLO('ciclo'),
        MES_FACTURA('mesFactura'),
        MES_ANTERIOR('mesAnterior'),
        MES_ACTUAL('mesActual'),
        TELEFONO('telefono'),
        RENTA_MENSUAL('rentaMensual'),
        PAQUETES_ADICIONALES('paquetesAdicionales'),
        FINANCIAMIENTOS('financiamientos'),
        INTERES_MORA('interesMora'),
        OTROS_CARGOS('otrosCargos'),
        AJUSTES('ajustes'),
        TOTAL_PAGO('totalPago'),
        FECHA_PAGO('fechaPago'),
        FACTURA_URL('facturaURL'),
        PAGO_URL('pagoURL'),
        PLAN('plan'),
        // -- Mail -- //
        BILL_AMOUNT('BILLMONTH'),
        MONEDA('MONEDA'),
        NUMFAC('NUMFAC'),
        ISSUEDDATE('ISSUEDDATE'),
        PHONE('PHONE'),
        MESFAC('MESFAC'),
        PERIFACINI('PERIFACINI'),
        PERFACFIN('PERFACFIN'),
        FULL_NAME('FIRSTNAME'),
        EMAIL('EMAIL'),
        SMART_URL('SMART_URL'),
        CLIENTE_CODIGO("CLIENTE_CODIGO"),
        CLIENTE_CODIGO_MD5("CLIENTE_CODIGO_MD5"),
        CLIENTE_NIT("CLIENTE_NIT"),
        VIDEO_URL("VIDEO_URL"),
        TELEFONIA("TELEFONIA"),
        ATTACHMENT("ATTACHMENT")

        String name

        CsvHeaderType(String name) {
            this.name = name;
        }

        static List headers() {
            values().collect({it.getName()})
        }
    }

    static String buildCsvRecord(TxtDigester.MultisectionRecord multisectionRecord) {
        def clienteCodigoV2 = multisectionRecord['0100000'].personId
        def clienteCodigoMD5 = CryptoUtil.calculateMD5(clienteCodigoV2)
        //def uidMD5 = CryptoUtil.calculateMD5(clienteCodigoV2 + multisectionRecord['0100000'].fechaEmision)
        def uidMD5 = TxtDigester.calcUidMD5(multisectionRecord)
        //def periodoFacturadoInicio = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_dd_MM_yyyy(multisectionRecord['0200000'].periodoFacturado)
        //def periodoFacturadoFin = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_dd_MM_yyyy(multisectionRecord['0100000'].fechaVencimiento)
        Integer ciclo = multisectionRecord['0100000'].ciclo.toInteger()
        def periodoFacturadoInicio = { // Format: dd/MM/yyyy
            String inicio
            //String fechaEmision = multisectionRecord['0100000'].fechaEmision
            String periodoFacturado = multisectionRecord['0200000'].periodoFacturado // dd/MMM/yyyy
            switch (TxtSpecs.CategoriaType.forCiclo(ciclo)) {
                case TxtSpecs.CategoriaType.CORPORATIVO:
                    if (ciclo == 2) {
                        inicio = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_firstDd_MMM_yyyy_fromCurrentMonth(periodoFacturado)
                    } else {
                        inicio = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_secondDd_MMM_yyyy_fromLastMonth(periodoFacturado)
                    }
                    break
                case TxtSpecs.CategoriaType.MASIVO:
                    //Período de Inicio = +1 día / -1 Mes
                    //02001POR VENTA DE SERVICIO TELEFONICO AL:  07/AGO/2021 (periodoFacturado)
                    //Por ejemplo, quedaría: 08/07/2021
                    inicio = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_plusOneDd_MM_yyyy_fromLastMonth(periodoFacturado)
                    break
            }
            return inicio
        }.call()
        def periodoFacturadoFin = { // Format: dd/MM/yyyy
            String fin
            //String fechaEmision = multisectionRecord['0100000'].fechaEmision
            String periodoFacturado = multisectionRecord['0200000'].periodoFacturado // dd/MMM/yyyy
            switch (TxtSpecs.CategoriaType.forCiclo(ciclo)) {
                case TxtSpecs.CategoriaType.CORPORATIVO:
                    if (ciclo == 2) {
                        fin = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_lastDd_MMM_yyyy_fromCurrentMonth(periodoFacturado)
                    } else {
                        fin = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_firstDd_MMM_yyyy_fromCurrentMonth(periodoFacturado)
                    }
                    break
                case TxtSpecs.CategoriaType.MASIVO:
                    //Período Fin = +1 día / -1 Mes
                    //010105TA AVENIDA 0-59                               2368-6381 Q            32.44   06/SEP/2021
                    //Por ejemplo, quedaría: 07/08/2021
                    def periodoFin = multisectionRecord['0100000'].fechaVencimiento // dd/MMM/yyyy
                    fin = FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_plusOneDd_MM_yyyy_fromLastMonth(periodoFin)
                    break
            }
            return fin
        }.call()
        def record = [] // new List
        // --- Doc1 --- //
        record << "\"A1000\""
        // -- Video -- //
        record << "\"${uidMD5}\"" // UID('uid')
        record << { // NAME('name')
            String clienteNombre = multisectionRecord['0100000'].receptorNombre.replaceAll('"', '')
            int comaMeter = 0
            String str = clienteNombre
            while (str.indexOf(',') != -1) {
                str = str.substring(str.indexOf(',') + 1)
                comaMeter += 1
            }
            def name
            if (comaMeter == 0) {
                name = clienteNombre.tokenize()[0]
            } else if (comaMeter > 2) {
                if (clienteNombre.contains(',,')) {
                    try {
                        name = clienteNombre.split(',,')[1].tokenize(',')[0]
                    } catch(exception) {
                        println "EXCEPTION ON clienteNombre: ${clienteNombre}"
                        return null
                    }
                } else {
                    name = clienteNombre.tokenize(',')[-1]
                }
            } else {
                name = clienteNombre.tokenize()[0]
            }
            return "\"${name}\""
        }.call()
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromCurrentMonth(multisectionRecord['0100000'].fechaEmision)}\"" // DIAS_ACTUAL('diasActual')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromLastMonth(multisectionRecord['0100000'].fechaEmision)}\"" // DIAS_ANTERIOR('diasAnterior')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd(periodoFacturadoInicio)}\"" // DIA_INICIO('diaInicio')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd(periodoFacturadoFin)}\"" // DIA_FINALIZA('diaFinaliza')
        record << "\"${multisectionRecord['0100000'].ciclo}\"" // CICLO('ciclo')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM(multisectionRecord['0100000'].fechaEmision)}\"" // MES_FACTURA('mesFactura')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM_fromLastMonth(multisectionRecord['0100000'].fechaEmision)}\"" // MES_ANTERIOR('mesAnterior')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM(multisectionRecord['0100000'].fechaEmision)}\"" // MES_ACTUAL('mesActual')
        record << "\"${multisectionRecord['0100000'].receptorTelefono}\"" // TELEFONO('telefono')
        record << '"0.00"' // RENTA_MENSUAL('rentaMensual')
        record << '"0.00"' // PAQUETES_ADICIONALES('paquetesAdicionales')
        record << '"?.??"' // "\"${multisectionRecord['100300'].totalFinanciamiento}\"" // FINANCIAMIENTOS('financiamientos')
        record << '"?.??"' // "\"${multisectionRecord['100300'].saldoMora}\"" // INTERES_MORA('interesMora')
        record << '"0.00"' // OTROS_CARGOS('otrosCargos')
        record << '"?.??"' // "\"${multisectionRecord['100300'].totalAjustes}\"" // AJUSTES('ajustes')
        //record << "\"${FormatUtil.currencyToNumberFormatter(multisectionRecord['0100000'].totalPagar)}\"" // TOTAL_PAGO('totalPago')
        record << "\"${FormatUtil.currencyToNumberFormatter(multisectionRecord['0200000'].totalPagar ?: '0.00')}\"" // TOTAL_PAGO('totalPago')
        //record << "\"${multisectionRecord['0100000'].fechaVencimiento}\"" // FECHA_PAGO('fechaPago')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_dd_MM_yyyy(multisectionRecord['0100000'].fechaVencimiento)}\"" // FECHA_PAGO('fechaPago')
        record << "\"BASE_URL/personaWeb/user/authhandoff?cde=${clienteCodigoMD5}\"" // FACTURA_URL('facturaURL')
        record << '"https://gt.mipagoclaro.com/?utm_source=FACTURA&utm_medium=VIDEO&utm_campaign=DOCONE#/"' // PAGO_URL('pagoURL')
        record << "\"${multisectionRecord['0100000'].servicio}\"" // PLAN('plan')
        // -- Mail -- //
        record << "\"${FormatUtil.currencyToNumberFormatter(multisectionRecord['0400200'].totalMes)}\"" // BILLMONTH
        record << '"Q"' // MONEDA
        //record << "\"${multisectionRecord['0100000'].documentoNumero}\"" //  NUMFAC
        record << "\"${multisectionRecord['0100000'].facturaSerie} ${multisectionRecord['0100000'].documentoNumeroV2}\"" //  NUMFAC
        record << "\"${multisectionRecord['0400200'].vencimiento}\"" // ISSUEDDATE
        record << "\"${multisectionRecord['0400200'].telefono}\"" // PHONE
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM_yyyy(multisectionRecord['0100000'].fechaEmision)}\"" // MESFAC
        switch (TxtSpecs.CategoriaType.forCiclo(ciclo)) {
            case TxtSpecs.CategoriaType.MASIVO:
                record << "\"${multisectionRecord['0200000'].periodoFacturado}\"" // PERIFACINI
                record << "\"${multisectionRecord['0100000'].fechaVencimiento}\"" // PERFACFIN
                break
            default:
                record << "\"${periodoFacturadoInicio}\"" // "\"${multisectionRecord['0200000'].periodoFacturado}\"" // PERIFACINI
                record << "\"${periodoFacturadoFin}\"" // "\"${multisectionRecord['0100000'].fechaVencimiento}\"" // PERFACFIN
        }
        record << "\"${multisectionRecord['0400200'].clienteNombre.replaceAll(',', ' ').replaceAll('  ', ' ').replaceAll('"', '')}\"" // FULLNAME
        record << "\"${multisectionRecord['0100000'].eMails}\"" // EMAIL
        record << "\"BASE_URL/personaWeb/user/authhandoff?cde=${clienteCodigoMD5}\"" // SMART_URL
        record << "\"${clienteCodigoV2}\""// CLIENTE_CODIGO("CLIENTE_CODIGO")
        record << "\"${clienteCodigoMD5}\""// CLIENTE_CODIGO_MD5("CLIENTE_CODIGO_MD5")
        record << "\"${multisectionRecord['0100000'].receptorNIT}\""// CLIENTE_NIT("CLIENTE_NIT")
        record << "\"https://us-east-1-mt-preprod2.engageone.video/claro/embed-iframe.php?uid=${uidMD5}\"" // VIDEO_URL("VIDEO_URL")
        record << '"FIJA"' // TELEFONIA("TELEFONIA")
        record << { // ATTACHMENT("ATTACHMENT")
            // Factura_[PersonID]_[Telefono].pdf
            def personId = multisectionRecord['0100000'].personId
            def telefono = multisectionRecord['0100000'].receptorTelefono.replace('-', '').padLeft(10,'0')
            "\"Factura_${personId}_${telefono}.pdf\""
        }.call()

        return record.join(',')
    }

    @Synchronized
    private static createDirectories(Path path) {
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent()) // Make sure the directories exist
        }
    }

    static void writeCsvToFile(Path filePath, String csvRecord) {
        createDirectories(filePath)
        filePath.withWriter('UTF-8') { writer ->
            writer << csvRecord
        }
    }

    static void generateMasterCsvFile(Path absDirPath) {
        println 'Generating MasterCSV file'

        if (Files.exists(absDirPath)) {
            String syntaxAndPattern = 'glob:**/*.csv'
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher(syntaxAndPattern)
            def files = Files.list(absDirPath).filter({ matcher.matches(it) }).collect(Collectors.toList())
            if (files.size() == 0) {
                println "There's no CSV files to generate a MasterCSV file"
                return
            }
        } else {
            println "There's no DIR to generate a MasterCSV file"
            return
        }

        Path csvFilePath = absDirPath.getParent().resolve("csv").resolve("${absDirPath.fileName.toString()}.csv")
        if (!Files.exists(csvFilePath.getParent())) {
            Files.createDirectories(csvFilePath.getParent()) // Make sure the directories exist
        }
        if (Files.exists(csvFilePath)) {
            println "Deleting existing file ${csvFilePath.toString()}"
            Files.delete(csvFilePath)
        }

        csvFilePath << CsvHeaderType.headers().join(',') + '\n'

        absDirPath.eachFileMatch(FileType.FILES, ~/.*\.csv$/) { filePath ->
            println "Merging ${filePath.fileName}"
            filePath.withReader { reader ->
                reader.eachLine { csvRecord ->
                    csvFilePath.withWriterAppend { writer ->
//                        splitCsvRecordByEmail(csvRecord).each {line ->
//                            writer << "${line}\n"
//                        }
                        writer << "${csvRecord}\n"
                    }
                }
            }
        }

        println "MasterCSV file ${csvFilePath} generated"
    }

    static void generateMasterCsvFileViaGO(Path absDirPath) {
        println 'Generating MasterCSV file via GO'
        List<String> command = ['go', 'run', '--work', 'generate_master_csv_file.go', absDirPath.toString()]
        ProcessUtil.doProcess(command)
    }

    private static List splitCsvRecordByEmail(String csvRecord) {
        def csvRecords = []
        def tokens = csvRecord.tokenize('||')
        def mails = tokens[1].replaceAll('"', '').tokenize(';')
        //println mails
        //println mails.size()
        if (mails) {
            mails.each {mail ->
                //println mail
                csvRecords << "${tokens[0]}\"${mail.trim()}\"${tokens[2]}"
            }
        } else {
            csvRecords << "${tokens[0]}\" \"${tokens[2]}"
        }
        return csvRecords
    }
}



class FormatUtil {
    static final Locale locale = new Locale('es', 'GT')

    static final NumberFormat NumberFormat_4dec = NumberFormat.getInstance(locale)
    static {
        NumberFormat_4dec.setRoundingMode(RoundingMode.HALF_EVEN)
        NumberFormat_4dec.setGroupingUsed(false)
        NumberFormat_4dec.setMinimumFractionDigits(4)
        NumberFormat_4dec.setMaximumFractionDigits(4)
    }

    static final NumberFormat NumberFormat_2dec = NumberFormat.getInstance(locale)
    static {
        NumberFormat_2dec.setRoundingMode(RoundingMode.HALF_EVEN)
        NumberFormat_2dec.setGroupingUsed(false)
        NumberFormat_2dec.setMinimumFractionDigits(2)
        NumberFormat_2dec.setMaximumFractionDigits(2)
    }

    static String currencyToNumberFormatter(String value) {
        return value.replace('Q','').trim().replace(',', '')
    }

    private static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_yyyyMMdd(String ddMMyyyy) {
        // dd/MM/yyyy -> yyyyMMdd
        LocalDate localDate = LocalDate.parse(ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy'))
        localDate.format(DateTimeFormatter.ofPattern('yyyyMMdd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_yyyyMMdd(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyyMMdd
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyyMMdd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_yyyyMM(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyyMM
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyyMM'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMMyyyy) {
        // dd/MM/yyyy -> yyyy-MM-dd
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyy-MM-dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyy-MM-dd
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyy-MM-dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMMM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyy-MM-dd
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMMM/yyyy', locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyy-MM-dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMM, String date_MMMMyyyy) {
        // dd/MM -> yyyy-MM-dd

        YearMonth yearMonth = YearMonth.parse(date_MMMMyyyy, DateTimeFormatter.ofPattern('MMMM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)

        String date_ddMMyyyy = "${date_ddMM}/${yearMonth.year}"
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyy-MM-dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM(String date_ddMMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_yyyy(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        String monthName_yyyy = yearMonth.format(DateTimeFormatter.ofPattern('MMMM yyyy', locale))
        return "${monthName_yyyy[0..<1].toUpperCase()}${monthName_yyyy[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM_yyyy(String date_ddMMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        String monthName_yyyy = yearMonth.format(DateTimeFormatter.ofPattern('MMMM yyyy', locale))
        return "${monthName_yyyy[0..<1].toUpperCase()}${monthName_yyyy[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_fromLastMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_MMMM_fromLastMonth(String date_ddMMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_dd_MM_yyyy(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    /*static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromCurrentMonth(String date_ddMMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        return "${yearMonth.lengthOfMonth()}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromLastMonth(String date_ddMMMyyyy) {
        // DiasAnterior
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        return "${yearMonth.lengthOfMonth()}"
    }*/

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromCurrentMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        return "${yearMonth.lengthOfMonth()}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromCurrentMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        return "${yearMonth.lengthOfMonth()}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_firstDd_MMM_yyyy_fromCurrentMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(1)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_secondDd_MMM_yyyy_fromCurrentMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(2)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_secondDd_MMM_yyyy_fromLastMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(2).minusMonths(1)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_lastDd_MMM_yyyy_fromCurrentMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(localDate.lengthOfMonth())
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_firstDD_MMM_yyyy_fromNextMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(1).plusMonths(1)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_firstDd_MM_yyyy_fromLastMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.withDayOfMonth(1).minusMonths(1)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedBySlash_plusOneDd_MM_yyyy_fromLastMonth(String date_ddMMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        localDate = localDate.plusDays(1).minusMonths(1)
        return localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromLastMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        return "${yearMonth.lengthOfMonth()}"
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_dd_fromLastMonth(String date_ddMMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMMyyyy.toLowerCase(), DateTimeFormatter.ofPattern('dd/MMM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        return "${yearMonth.lengthOfMonth()}"
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd(String date_ddMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        return localDate.format(DateTimeFormatter.ofPattern('dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMM(String date_ddMMyyyy) {
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        return localDate.format(DateTimeFormatter.ofPattern('MMM'))
    }

    static String timeFormatterSeparatedByColon_HH_mm_ss(String HHmmss) {
        // HH:mm:ss -> hh:mm:ss AM|PM
        LocalTime localTime = LocalTime.parse(HHmmss)
        return localTime.format(DateTimeFormatter.ofPattern('hh:mm:ss a'))
    }

    static String numberToLetters(String number) {
        def units   = ['cero','uno','dos','tres','cuatro','cinco','seis','siete','ocho','nueve']
        def decs    = ['x','y','veinte','treinta','cuarenta','cincuenta','sesenta','setenta','ochenta','noventa']
        def dieces  = ['diez','once','doce','trece','catorce','quince','dieciséis','diecisiete','dieciocho','diecinueve']
        def cientos = ['x','cien','doscientos','trescientos','cuatrocientos','quinientos','seiscientos','setecientos','ochocientos','novecientos']

        Integer entero
        String decimales
		// Roberto / Jasiel
		String negativo = ''

        if(number.contains('-')){
            negativo = 'MENOS'
            number = number.replace("-","")
        }

        if (number.contains('.')) {
            int punto = number.indexOf('.')
            if ( punto == -1 ) {
                entero = number.toInteger()
                //decimales = '00/100'
                decimales = 'QUETZALES EXACTOS'
            } else {
                entero = number.take(punto).toInteger()

                def decimal = number.substring(punto +1)

                if (decimal.toInteger() == 0) {
                    decimales = 'QUETZALES EXACTOS'
                    //} else if (decimal.length() == 1) {
                    //    decimales = decimal + '0/100'
                } else {
                    //decimales = decimal.take(2) + '/100' // Se trunca a dos decimales
                    decimales = "QUETZALES CON ${decimal.take(2)}/100" // Se trunca a dos decimales
                }
            }
        } else {
            entero = number.toInteger()
        }

        Integer millones = entero.intdiv(1000000)
        Integer millares = entero.intdiv(1000) % 1000
        Integer centenas = entero.intdiv(100) % 10
        Integer decenas = entero.intdiv(10) % 10
        Integer unidades = entero % 10

        StringBuilder letras = new StringBuilder()

        if (millones == 1) {
            letras << 'un millón'
        } else if (millones > 1) {
            letras.append(numberToLetters(millones.toString())).append ' millones' // recursive call
        }

        if (millares == 1) {
            letras << ' un mil' // o 'mil', si no se quiere tipo 'moneda'
        } else if (millares > 0) {
            letras.append(' ').append(numberToLetters(millares.toString())).append ' mil' // recursive call
        }

        if (centenas == 1) {
            letras << (entero % 100 == 0 ? ' cien' : ' ciento')
        } else if (centenas > 0) {
            letras.append(' ').append(cientos[centenas])
        }

        if (decenas == 1) {
            letras.append(' ').append(dieces[entero % 10])
            unidades = 0
        } else if (decenas == 2 && unidades > 0) {
            if (unidades == 6) {
                letras.append(' veinti').append('séis')
            } else {
                letras.append(' veinti').append(units[unidades])
            }
            unidades = 0
        } else if (decenas > 1) {
            letras.append(' ').append(decs[decenas])

            if (unidades > 0) {
                letras << ' y'
            }
        }

        if (unidades > 0) {
            letras.append(' ').append units[unidades]
        } else if (entero == 0) {
            letras << units[0]
        }

		// Roberto / Jasiel
        //if (number.contains('.')) {
        //    return "${letras.toString().trim().toUpperCase()} ${decimales}"
        //} else {
        //    return "${letras.toString().trim().toUpperCase()}"
        //}
        if (number.contains('.')) {
            return "$negativo ${letras.toString().trim().toUpperCase()} ${decimales}"
        } else {
            return "$negativo ${letras.toString().trim().toUpperCase()}"
        }
    }
}


class JsonUtil {
    @Synchronized
    private static createDirectories(Path path) {
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent()) // Make sure the directories exist
        }
    }

    static void writeJsonToFile(Path filePath, String json) {
        createDirectories(filePath)
        filePath.withWriter('UTF-8') { writer ->
            writer << json
        }
    }
}



final class FilenameUtil {

    static Tuple splitPathname(String pathname) {
        Path filePath = Paths.get(pathname)

        String absoluteDirName = filePath.getParent().toString()
        String filenameWithExt = filePath.fileName
        String filenameWithoutExt = filenameWithExt.substring(0, filenameWithExt.lastIndexOf("."))
        String fileExt = filenameWithExt.substring(filenameWithExt.lastIndexOf(".") + 1)

        new Tuple(absoluteDirName, filenameWithoutExt, fileExt)
    }

    static Path createDirBasedOnPathname(String pathname) {
        Tuple tuple = splitPathname(pathname)
        String absoluteDirName = tuple.get(0)
        String filenameWithoutExt = tuple.get(1)
        //String fileExt = tuple.get(2)

        return Paths.get(absoluteDirName).resolve(filenameWithoutExt)
    }
}



class CryptoUtil {
    static String calculateMD5(String s){
        MessageDigest.getInstance("MD5").digest(s.bytes).encodeHex().toString()
    }

    static String calculateCRC32(String s) {
        byte[] data = s.getBytes()
        Checksum checksum = new CRC32()
        checksum.update(data, 0, data.length)
        long checksumValue = checksum.getValue()
        String hex = Long.toHexString(checksumValue).toUpperCase()
        while (hex.length() < 8) {
            hex = "0" + hex
        }
        return hex
    }
}



final class ProcessUtil {
    static String doProcess(List<String> command) {
        ProcessBuilder pb = new ProcessBuilder(command)
        pb.redirectErrorStream(true)
        Process proc = pb.start()

        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line
        String result
        while ( (line = reader.readLine()) != null) {
            println line
            result = line
        }

        if (proc.exitValue() == 0) {
            return result
        } else {
            return null
        }
    }
}



class Actors {
    enum MessageType {
        GIMME_TASK,
        NO_MORE_TASK
    }

    static final class Masterworker extends DefaultActor {
        static final MAX_PIECEWORKERS_SIZE = Runtime.getRuntime().availableProcessors()

        private final List pieceworkers = []
        private Path absDirPath
        private int filesMaxSize
        private int filesMeter = 1 // initial value

        Masterworker(Binding binding, Path absDirPath, int filesSize) {
            this.absDirPath = absDirPath
            this.filesMaxSize = filesSize
            // --
            println "${this.class.canonicalName} HIRED to execute special tasks that requires its talents."
            println 'Hiring pieceworkers...'
            (1..MAX_PIECEWORKERS_SIZE).each { index ->
                this.pieceworkers << new Pieceworker(binding, this, index, absDirPath)
            }
        }

        void afterStart() {
            println 'Starting pieceworkers...'
            pieceworkers*.start()
        }
        void afterStop(List undeliveredMessages) {
            println "${this.class.canonicalName} has DONE the work. FAREWELL"
        }
        void onInterrupt(InterruptedException e) {}
        void onTimeout() {}
        void onException(Throwable e) { e.printStackTrace() }

        void act() {
            loop {
                react { message ->
                    switch (message) {
                        case MessageType.GIMME_TASK.name():
                            if (this.filesMeter > this.filesMaxSize) {
                                println "No more tasks to do. Firing the pieceworker[${sender.id}]."
                                reply MessageType.NO_MORE_TASK.name()
                            } else {
                                // Get a collection of strings from file filename_[idx].txt
                                String filename = "${this.absDirPath.getFileName().toString()}_[${this.filesMeter++}].txt"
                                Path filePath = this.absDirPath.resolve(filename)
                                List<String> lines = filePath.readLines('ISO-8859-1')
                                reply lines
                            }
                            break
                    }
                }
            }
        }
    }

    static final class Pieceworker extends DefaultActor {
        private Binding binding
        private Integer id
        private Actor patron
        private boolean hired
        private final Path absDirPath

        Pieceworker(Binding binding, Actor patron, Integer id, Path absDirPath) {
            this.binding = binding
            this.patron = patron
            this.id = id
            this.absDirPath = absDirPath
            hired = true
            println "${this.class.canonicalName}[${id}] HIRED"
        }

        void act() {
            loop {
                println "${this.class.canonicalName}[${id}] requests task to do."
                if (hired) {
                    this.patron << MessageType.GIMME_TASK.name()
                }
                react { message ->
                    switch (message) {
                        case MessageType.NO_MORE_TASK.name():
                            println "${this.class.canonicalName}[${id}] FIRED"
                            hired = false
                            this.terminate()
                            break
                        default:
                            processLines(message)
                    }
                }
            }
        }

        private void processLines(List<String> lines) {
            TxtDigester txtDigester = new TxtDigester(TxtSpecs.SECTIONS) //(TxtSpecs.FIELDS, TxtSpecs.TABLES)
            TxtDigester.MultisectionRecord multisectionRecord = txtDigester.buildMultisectionRecord(lines, this.id.toString())
            if (multisectionRecord == null) {
                // No task, then wait
                Thread.sleep 1000
                return
            }

            println 'SO FAR, SO GOOD'

            // NOTE: Block only for development purposes
//            String multisectionRecordJsonPrettyPrint = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multisectionRecord))
//            //println multisectionRecordJsonPrettyPrint
//            if (multisectionRecordJsonPrettyPrint) {
//                String jsonFilename = "${multisectionRecord['0100000'].documentoNumeroV2}.json"
//                Path jsonFilePath = this.absDirPath.resolve(jsonFilename)
//                JsonUtil.writeJsonToFile(jsonFilePath, multisectionRecordJsonPrettyPrint)
//            }

            String xml = XmlUtil.buildXml(multisectionRecord)
            if (xml) {
                // println groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multilineRecord.B1000[0]))
                String xmlFilename = "${multisectionRecord['0100000'].documentoNumeroV2}.xml"
                Path xmlFilePath = this.absDirPath.resolve(xmlFilename)
                XmlUtil.writeXmlToFile(xmlFilePath, xml)
                // --
                xml = XmlUtil.buildChildrenDoc1(multisectionRecord)
                if (xml) {
                    xmlFilename = "${multisectionRecord['0100000'].documentoNumeroV2}-children.doc1"
                    xmlFilePath = this.absDirPath.resolve(xmlFilename)
                    XmlUtil.writeXmlToFile(xmlFilePath, xml)
                }
                // --
                xml = XmlUtil.buildChildrenSmartbill(multisectionRecord)
                if (xml) {
                    xmlFilename = "${multisectionRecord['0100000'].documentoNumeroV2}-children.smartbill"
                    xmlFilePath = this.absDirPath.resolve(xmlFilename)
                    XmlUtil.writeXmlToFile(xmlFilePath, xml)
                }
            }

            String csvRecord = CsvUtil.buildCsvRecord(multisectionRecord)
            if (csvRecord) {
                String csvFilename = "${multisectionRecord['0100000'].documentoNumeroV2}.csv"
                Path csvFilePath = this.absDirPath.resolve(csvFilename)
                CsvUtil.writeCsvToFile(csvFilePath, csvRecord)
            }
        }
    }
}



class SpoolSplitter {

    static int splitFileViaGO(Path filePath) {
        println 'Splitting SPOOL via GO'
        int filesSize = 0
        List<String> command = ['go', 'run', '--work', 'spool_splitter.go', filePath.toString()]
        String result = ProcessUtil.doProcess(command)
        if (result != null) {
            filesSize = result.toInteger()
        }
        return filesSize
    }

    static int splitFile(Path filePath) {
        println "Splitting ${filePath}"

        int fileMeter = 0
        int lineMeter = 0
        File file

        filePath.withReader('ISO-8859-1') { reader ->
            reader.eachLine { line ->
                if (line.startsWith(TxtSpecs.SectionType._0100000.id)) {
                    fileMeter += 1
                    file = createFile(filePath, fileMeter)
                }
                lineMeter += 1
                file.withWriterAppend('ISO-8859-1') { writer ->
                    writer << "${line}\n"
                }
            }
        }

        println "File ${filePath.getFileName().toString()} of ${lineMeter} lines splitted into ${fileMeter} files"

        return fileMeter
    }

    static File createFile(Path filePath, Integer fileIndex) {
        def tuple = splitPathname(filePath.toString())
        String absoluteDirName = tuple.get(0)
        String filenameWithoutExt = tuple.get(1)
        def fileExt = tuple.get(2)

        Path dirPath = Paths.get(absoluteDirName)
        Path newDirPath = dirPath.resolve(filenameWithoutExt)
        if (!Files.exists(newDirPath)) {
            Files.createDirectories(newDirPath) // Make sure the directories exist
        }

        Path newFilePath
        if (fileIndex) {
            newFilePath = newDirPath.resolve("${filenameWithoutExt}_[${fileIndex}].${fileExt}")
        } else {
            newFilePath = newDirPath.resolve("${filenameWithoutExt}.${fileExt}")
        }
        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath)
        }

        println newFilePath.getFileName().toString()
        return newFilePath.toFile()
    }

    static Tuple splitPathname(String pathname) {
        Path filePath = Paths.get(pathname)

        String absoluteDirName = filePath.getParent().toString()
        String filenameWithExt = filePath.fileName
        String filenameWithoutExt = filenameWithExt.substring(0, filenameWithExt.lastIndexOf("."))
        String fileExt = filenameWithExt.substring(filenameWithExt.lastIndexOf(".") + 1)

        return new Tuple(absoluteDirName, filenameWithoutExt, fileExt)
    }
}



class Globals {
    static String LINE_ERROR = "I'm global.."
}


// ------------------------------------------------------------------
// Main
// ------------------------------------------------------------------

Date GLOBAL_START = new Date()

// ===== DEV ===== //
def mainDEV() {
    String filePathname
    if (binding.variables.args) {
        filePathname = binding.variables.args.first()
    } else {
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\20220411092847_CLAROGTFIJA_GFTX220408_extracto.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\GFTX_TXT_202102_Fija_Muestras_20210803_extracto.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\CLAROGTFIJA_GFTX220408_MUESTRA_FINAN.txt'

        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\CLAROGTFIJA_GFTX220711_PBA_NEGATIVOS.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\CLAROGTFIJA_GFTX220658.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\MUESTRA_CLAROGTFIJA_GFTX220802.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\MUESTRA_CLAROGTFIJA_GFTX220858.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\MUESTRA_CLAROGTFIJA_GFTM211008.txt' // Financiamientos
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\CLAROGTFIJA_gftx220508_importe_Ajustes.txt' // Financiamientos
        filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_FIJA\\20221012235537_CLAROGTFIJA_GFTX221008.txt' // Financiamientos
    }

    Path absDirPath = FilenameUtil.createDirBasedOnPathname(filePathname)

    def status = null
    Integer filesSize = 0
    Path statusFilePath = absDirPath.getParent().resolve(absDirPath.toString() + ".status")
    if (Files.exists(statusFilePath)) {
        def fileLines = statusFilePath.readLines()
        status = fileLines.size() > 1 ? fileLines[0] : null
        filesSize = fileLines.size() > 1 ? fileLines[1].toInteger() : 0
    }

    if (status != 'PROCESSED') {
        // Split filePath and keep track of max index (for TaskProducer constructor)
        Path filePath = Paths.get(filePathname)
        //int filesSize = SpoolSplitter.splitFile(filePath)
        if (filesSize == 0) {
            //filesSize = SpoolSplitter.splitFile(filePath)
            filesSize = SpoolSplitter.splitFileViaGO(filePath)

            // Write status into file
            statusFilePath.write('SPLIT\n')
            statusFilePath.append("${filesSize}")
        }

        final Actor masterworker = new Actors.Masterworker(binding, absDirPath, filesSize).start()
        masterworker.pieceworkers*.join()
        masterworker.stop()
        masterworker.join()

        // Process concurrently
        GParsPool.withPool(3) {
            //XmlUtil.&generateDoc1File.callAsync(absDirPath)
            //XmlUtil.&generateSmartbillFile.callAsync(absDirPath)
            //CsvUtil.&generateMasterCsvFile.callAsync(absDirPath)

            XmlUtil.&generateDoc1FileViaGO.callAsync(absDirPath)
            XmlUtil.&generateSmartbillFileViaGO.callAsync(absDirPath)
            CsvUtil.&generateMasterCsvFileViaGO.callAsync(absDirPath)
        }

        statusFilePath.write('PROCESSED')
        println "File ${filePathname} PROCESSED"
    } else {
        println "File ${filePathname} previsouly PROCESSED"
    }
}

// ===== SPECTRUM ===== //
def mainSPECTRUM() {

    TxtDigester txtDigester = new TxtDigester(TxtSpecs.SECTIONS)
    TxtDigester.MultisectionRecord multisectionRecord = txtDigester.buildMultisectionRecord(data.lines)
    if (multisectionRecord) {
        String mainXml = XmlUtil.buildXml(multisectionRecord)
        String childrenDoc1 = XmlUtil.buildChildrenDoc1(multisectionRecord)
        String childrenSmartbill = XmlUtil.buildChildrenSmartbill(multisectionRecord)
        String csvRecord = CsvUtil.buildCsvRecord(multisectionRecord)
        if (mainXml) {
            //println groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multilineRecord.B1000[0]))
            data.mainXml = mainXml
            data.childrenDoc1 = childrenDoc1
            data.childrenSmartbill = childrenSmartbill
            data.csvRecord = csvRecord
            data.personId = multisectionRecord['0100000'].personId
            data.imprimeCiclo = multisectionRecord['0100000'].imprimeCiclo // 20221116 Jasiel
            data.categoria = TxtSpecs.CategoriaType.forCiclo(multisectionRecord['0100000'].ciclo.toInteger()).name()
        } else {
            data.mainXml = ''
            data.childrenDoc1 = ''
            data.childrenSmartbill = ''
            data.csvRecord = ''
            data.personId = ''
            data.imprimeCiclo = 'N' // 20221116 Jasiel
            data.categoria = ''
        }
    }

}

// ----------------------
// SPECTRUM data BINDING
// ----------------------
if (binding.hasVariable('data')) {
    mainSPECTRUM()
} else {
    mainDEV()
}

println "***Whole process took ${TimeCategory.minus(new Date(), GLOBAL_START)}"
