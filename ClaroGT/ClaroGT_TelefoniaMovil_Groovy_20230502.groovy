import groovy.io.FileType
import groovy.time.TimeCategory
import groovy.transform.Synchronized
import groovy.xml.MarkupBuilder
import groovyx.gpars.GParsPool
import groovyx.gpars.actor.Actor
import groovyx.gpars.actor.DefaultActor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

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
import java.time.temporal.TemporalField
import java.util.stream.Collectors
import java.util.zip.CRC32
import java.util.zip.Checksum

class Globals {
    static String VERSION = 'claroHN_movil_v1'
    static String LINE_ERROR = "I'm global.."
    //static String LINE_ERROR = "I'm global.."
    //Logger logger = LoggerFactory.getLogger(this.class); // this = groovy script
    //Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    //Logger log = LoggerFactory.getLogger("STDOUT");//any logger name @ server/conf/logback.xml;
    static Logger log = LoggerFactory.getLogger("STDOUT");//any logger name @ server/conf/logback.xml;
    //groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(data))
}



class TxtSpecs {

    enum OrderType { // 300101
        INCLUIDOS(/.*INCLUIDOS.*/, '001'), // DETALLE DE USO DE LOS SERVICIOS INCLUIDOS DENTRO DEL PLAN CONTRATADO
        EXCEDENTES(/.*EXCEDENTES.*/, '002'),
        AGREGADO(/.*AGREGADO/, '003'), // DETALLE DE SERVICIOS DE VALOR AGREGADO
        ET_AL('ET AL', '999')

        String regex
        String order

        OrderType(String regex, String order) {
            this.regex = regex
            this.order = order
        }

        static OrderType forKeyword(String str) {
            for(OrderType type : values()) {
                if (str ==~ type.regex) {
                    return type
                }
            }
            return ET_AL
        }
    }

    enum SubOrderType { // 300102
        LLAMADAS_LOCAL(/.*LLAMADAS.*LOCAL/, '001'),
        MENSAJES_LOCAL(/.*MENSAJES.*LOCAL/, '002'),
        LLAMADAS_INTERNACIONAL(/.*LLAMADAS.*INTERNACIONAL.*/, '003'),
        MENSAJES_INTERNACIONAL(/.*MENSAJES.*INTERNACIONAL.*/, '004'),
        EVENTOS_INTERNACIONAL(/.*MENSAJES.*INTERNACIONAL.*/, '005'), // !!!!!!!!
        //3001022DETALLE DE LLAMADAS EN ROAMING
        //3001022DETALLE DE LLAMADAS EN ROAMING SIN FRONTERAS
        LLAMADAS_ROAMING(/.*LLAMADAS.*ROAMING.*/, '006'),
        MENSAJES_ROAMING(/.*MENSAJES.*ROAMING.*/, '007'),
        ET_AL('ET AL', '999')

        String regex
        String order

        SubOrderType(String regex, String order) {
            this.regex = regex
            this.order = order
        }

        static SubOrderType forKeyword(String str) {
            for(SubOrderType type : values()) {
                if (str ==~ type.regex) {
                    return type
                }
            }
            return ET_AL
        }
    }

    enum ConsumoType { // 3001033
        LLAMADA(['MM:SS/LLAM', 'MM:SS']),
        MENSAJE(['MENSAJE', 'MENSAJES ENVIADOS', 'NONE']),
        DATO(['KILOBYTES TRANSMITIDOS']),
        EVENTO(['EVENTO'])

        List desc

        ConsumoType(List desc) {
            this.desc = desc
        }

        static ConsumoType forDesc(String name) throws Exception {
            for(ConsumoType type : values()) {
                if (type.desc.contains(name)) {
                    return type
                }
            }
            throw new Exception("ConsumoType NO identificado: ${name}")
        }
    }

    static int LINE_ID_LENGTH = 6

    enum SectionType {
        _000000('000000', ), // DESCONOCIDO
        _100000('100000', ), // Encabezado
        _100100('100100', ), // Emisor
        _100200('100200', ), // Receptor
        _100300('100300', ), // Totales
        _100400('100400', ), // Personalizados
        _100500('100500', ), // Mercadeo
        _100600('100600', ), // Consolidado
        _200000('200000', ), // DetalleSubCuentas
        _200100('200100', ), // DetalleCargosAdicionales
        _200200('200200', ), // DetalleServicios
        _200300('200300', ), // DetalleServAgrupados
        _200400('200400', ), // DetalleDeConectividad
        _200500('200500', ), // DetalleFinanciamientos
        _200600('200600', ), // DetalleConvenios
        _300000('300000', ), // DetalleLlamadas
        _400000('400000', ), // Ajustes
        //_900000('900000', ), // ???
        _999999('999999', ) // End of Multisection

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
            return _000000;
        }

        static boolean startsNewSectionExcludingVeryFirstSection(String id) {
            SectionType type = forId(id)
            return type != _100000 && type != _000000
        }
        //boolean startsNewSection = startsNewSectionClosure.call(lineId)


        static boolean endOfMultilineSection(String id) {
            return id == _999999.id
        }

        static boolean endOfMultilineSection(SectionType sectionType) {
            return sectionType == _999999
        }
    }

    private static final  SECTIONS = [
        '100000' : [ // Encabezado
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100000/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100001/:
                                sectionRecord.documentoTipo = line[34..-1].trim()
                                break
                            case ~/^100002/:
                                sectionRecord.documentoNumero = line[34..-1].trim()
                                break
                            case ~/^100003/:
                                sectionRecord.clienteCategoria = line[34..-1].trim()
                                break
                            case ~/^100004/:
                                sectionRecord.clienteCiclo = line[34..-1].trim()
                                break
                            case ~/^100005/:
                                sectionRecord.periodoInicio = line[34..-1].trim()
                                break
                            case ~/^100006/:
                                sectionRecord.periodoFin = line[34..-1].trim()
                                break
                            case ~/^100007/:
                                sectionRecord.fechaAcreditacion = line[34..-1].trim()
                                break
                            case ~/^100008/:
                                sectionRecord.fechaEmision = line[34..-1].trim()
                                break
                            case ~/^100009/:
                                sectionRecord.fechaLimitePago = line[34..-1].trim()
                                break
                            case ~/^100010/:
                                sectionRecord.noContratos = line[34..-1].trim().trim()
                                break
                            case ~/^100011/:
                                sectionRecord.serie = line[34..-1].trim()
                                break
                            case ~/^100012/:
                                sectionRecord.preImpreso = line[34..-1].trim()
                                break
                            case ~/^100013/:
                                sectionRecord.numeroAutorizacion = line[34..-1].trim()
                                break
                            case ~/^100014/:
                                sectionRecord.serieAdministrativa = line[34..-1].trim()
                                break
                            case ~/^100015/:
                                sectionRecord.numeroAdministrativo = line[34..-1].trim()
                                break
                            case ~/^100016/:
                                sectionRecord.detalleTrafico = line[34..-1].trim()
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
        '100100' : [ // Emisor
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord(/*id: lines[0].take(LINE_ID_LENGTH)*/)
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100100/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100101/:
                                sectionRecord.nombreEmisor = line[34..-1].trim()
                                break
                            case ~/^100102/:
                                sectionRecord.numeroTributarioEmisor = line[34..-1].trim()
                                break
                            case ~/^100103/:
                                sectionRecord.nombreComercialEmisor = line[34..-1].trim()
                                break
                            case ~/^100104/:
                                sectionRecord.direccionEmisor = line[34..-1].trim()
                                break
                            case ~/^100105/:
                                sectionRecord.telefonoEmisor = line[34..-1].trim()
                                break
                            case ~/^100106/:
                                sectionRecord.faxEmisor = line[34..-1].trim()
                                break
                            case ~/^100107/:
                                sectionRecord.emailEmisor = line[34..-1].trim()
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
        '100200' : [ // Receptor
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord(/*id: lines[0].take(LINE_ID_LENGTH)*/)
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100200/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100201/:
                                sectionRecord.clienteNombre = line[34..-1].trim()
                                break
                            case ~/^100202/:
                                sectionRecord.clienteCodigo = line[34..-1].trim()
                                break
                            case ~/^100203/:
                                sectionRecord.clienteId = line[34..-1].trim()
                                break
                            case ~/^100204/:
                                sectionRecord.clienteDireccionL1 = line[34..-1].trim()
                                break
                            case ~/^100205/:
                                sectionRecord.clienteDireccionL2 = line[34..-1].trim()
                                break
                            case ~/^100206/:
                                sectionRecord.clienteDireccionL3 = line[34..-1].trim()
                                break
                            case ~/^100207/:
                                sectionRecord.clienteTelefono = line[34..-1].trim()
                                break
                            case ~/^100208/:
                                sectionRecord.clienteEmail = line[34..-1].trim()
                                break
                            case ~/^100209/:
                                sectionRecord.clienteNumeroTributario = line[34..-1].trim()
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
        '100300' : [ // Totales
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100300/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100301/:
                                sectionRecord.moneda = line[34..-1].trim()
                                break
                            case ~/^100302/:
                                sectionRecord.saldoCorteAnterior = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100303/:
                                sectionRecord.totalAjustes = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100304/:
                                sectionRecord.totalPagos = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100305/:
                                sectionRecord.saldoMora = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100306/:
                                sectionRecord.totalServicios = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100307/:
                                sectionRecord.totalImpuestoVenta = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100308/:
                                sectionRecord.total = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100309/:
                                sectionRecord.totalExento = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100310/:
                                sectionRecord.totalFinanciamiento = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100311/:
                                sectionRecord.totalConvenioPagos = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100312/:
                                sectionRecord.totalFinal = FormatUtil.currencyToNumberFormatter(line[34..-1])
                                break
                            case ~/^100313/:
                                sectionRecord.totalLetras = line[34..-1].trim()
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
        '100400' : [ // Personalizados
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100400/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100401/:
                                sectionRecord.codigoBarras = line[34..-1].trim()
                                break
                            case ~/^100402/:
                                sectionRecord.firmaElectronica = line[34..-1].trim()
                                break
                            case ~/^100403/:
                                sectionRecord.courierCodigo = line[34..-1].trim()
                                break
                            case ~/^100404/:
                                sectionRecord.courierNombre = line[34..-1].trim()
                                break
                            case ~/^100405/:
                                sectionRecord.courierRuta = line[34..-1].trim()
                                break
                            case ~/^100406/:
                                sectionRecord.enviarCorreo = line[34..-1].trim()
                                break
                            case ~/^100407/:
                                sectionRecord.imprimirPeriodo = line[34..-1].trim()
                                break
                            case ~/^100408/:
                                sectionRecord.resolucion = line[34..-1].trim()
                                break
                            case ~/^100409/:
                                sectionRecord.sistemaEmisor = line[34..-1].trim()
                                break
                            case ~/^100410/:
                                sectionRecord.observaciones = line[34..-1].trim()
                                break
                            case ~/^100430/:
                                sectionRecord.correlativo = line[34..-1].trim()
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
        '100500' : [ // Mensajes Mercadeo
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100500/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100501/:
                                sectionRecord.txt1 = line[34..-1].trim()
                                break
                            case ~/^100502/:
                                sectionRecord.txt2 = line[34..-1].trim()
                                break
                            case ~/^100503/:
                                sectionRecord.txt3 = line[34..-1].trim()
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
        '100600' : [ // Consolidado
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord['consolidados'] = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^100600/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^100602/: // Collection
                                def lineV2 = line[7..-1]
                                def tokens = lineV2.split(' Q ')
                                // --
                                TxtDigester.SectionRecord subSectionRecord = new TxtDigester.SectionRecord()
                                subSectionRecord.nombre = tokens[0].trim()
                                subSectionRecord.total = FormatUtil.currencyToNumberFormatter(tokens[1])
                                subSectionRecord.descuento = FormatUtil.currencyToNumberFormatter(tokens[2])
                                subSectionRecord.totalFinal = FormatUtil.currencyToNumberFormatter(tokens[3])
                                sectionRecord['consolidados'] << subSectionRecord
                                break
                            case ~/^100603/: // TOTAL FACTURA
                                def lineV2 = line[7..-1]
                                def tokens = lineV2.split(' Q ')
                                // --
                                TxtDigester.SectionRecord subSectionRecord = new TxtDigester.SectionRecord()
                                subSectionRecord.nombre = tokens[0].trim()
                                subSectionRecord.total = FormatUtil.currencyToNumberFormatter(tokens[1])
                                subSectionRecord.descuento = FormatUtil.currencyToNumberFormatter(tokens[2])
                                subSectionRecord.totalFinal = FormatUtil.currencyToNumberFormatter(tokens[3])
                                // --
                                //sectionRecord.totalFactura = subSectionRecord
                                sectionRecord['consolidados'] << subSectionRecord
                                break
                            case ~/^100604/: // AHORRO DEL MES (DESCUENTO)
                                // NOTA:
                                // El item on ID 4 "1006044AHORRO DEL MES (DESCUENTO)" no se está usando, por lo que NO
                                // se muestra en la factura, sin importar que el importe venga en CERO o diferente de CERO
//                                def lineV2 = line[7..-1]
//                                def tokens = lineV2.split(' Q ')
//                                // --
//                                TxtDigester.SectionRecord subSectionRecord = new TxtDigester.SectionRecord()
//                                subSectionRecord.nombre = tokens[0].trim()
//                                subSectionRecord.total = ''
//                                subSectionRecord.descuento = ''
//                                subSectionRecord.totalFinal = FormatUtil.currencyFormatter(tokens[1])
//                                // --
//                                //sectionRecord.descuento = subSectionRecord
//                                sectionRecord['consolidados'] << subSectionRecord
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
        '200000' : [ // DetalleSubCuentas
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord(/*id: lines[0].take(LINE_ID_LENGTH)*/)
                sectionRecord['detalleSubCuentas'] = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200000/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200003/:
                                def lineV2 = line[7..-1]
                                def tokensV1 = lineV2.split(' Q ')
                                def tokensV2 = tokensV1.join(' ').tokenize()
                                def valor = tokensV2.pop() // Takes and removes last element
                                def codigo = tokensV2.first() // Takes first element
                                tokensV2.remove(0) // Removes first element
                                def nombre = tokensV2.join(' ')
                                // --
                                TxtDigester.SectionRecord subSectionRecord = new TxtDigester.SectionRecord(id: id)
                                subSectionRecord.nombre = nombre
                                subSectionRecord.valor = FormatUtil.currencyToNumberFormatter(valor)
                                subSectionRecord.codigo = codigo
                                sectionRecord['detalleSubCuentas'] << subSectionRecord
                                break
                            case ~/^200004/: // TOTAL
                                sectionRecord.total = FormatUtil.currencyToNumberFormatter(line.tokenize(' Q ')[1])
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
        '200100' : [ // DetalleCargosAdicionalesYPromociones
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.detalleCargosAdicionalesYPromociones = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200100/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200101/:
                                TxtDigester.SectionRecord tmpSectionRecord = new TxtDigester.SectionRecord()
                                tmpSectionRecord.codigo = line[7..-1].trim()
                                sectionRecord.detalleCargosAdicionalesYPromociones << tmpSectionRecord
                                break
                            case ~/^200102/:
                                sectionRecord.detalleCargosAdicionalesYPromociones.last()['nombre'] = line[7..-1].trim()
                                sectionRecord.detalleCargosAdicionalesYPromociones.last()['cargosAdicionalesYPromociones'] = []
                                break
                            case ~/^200103/:
                                // NTERESES POR MORA
                                // ET AL
                                def tokens = line[7..-1].split(' Q ')
                                // --
                                TxtDigester.SectionRecord tmpSubSectionRecord = new TxtDigester.SectionRecord()
                                tmpSubSectionRecord.cargoAdicionalYPromocionNombre = tokens[0].trim()
                                tmpSubSectionRecord.cargoAdicionalYPromocionTotal = FormatUtil.currencyToNumberFormatter(tokens[1])
                                tmpSubSectionRecord.cargoAdicionalYPromocionItems = [] // new List
                                // --
                                sectionRecord.detalleCargosAdicionalesYPromociones.last()['cargosAdicionalesYPromociones'] << tmpSubSectionRecord
                                break
                            case ~/^200104/:
                                //sectionRecord.interesesMoraValor = FormatUtil.currencyFormatter(line[76..-1])
                                def tokens = line[7..-1].split(' Q ')
                                TxtDigester.SectionRecord itemSubSectionRecord = new TxtDigester.SectionRecord()
                                itemSubSectionRecord.cargoAdicionalYPromocionItemNombre = tokens[0].trim()
                                itemSubSectionRecord.cargoAdicionalYPromocionItemValor = FormatUtil.currencyToNumberFormatter(tokens[1])
                                sectionRecord.detalleCargosAdicionalesYPromociones.last()['cargosAdicionalesYPromociones'].last()['cargoAdicionalYPromocionItems'] << itemSubSectionRecord
                                break
                            case ~/^200105/:
                                def tokens = line[7..-1].split(' Q ')
                                sectionRecord.detalleCargosAdicionalesYPromociones.last()['totalPagar'] = FormatUtil.currencyToNumberFormatter(tokens[1])
                                //sectionRecord.totalPagar = FormatUtil.currencyFormatter(tokens[1])
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
        '200200' : [ // DetalleServicios
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.periodoInicio = ''
                sectionRecord.periodoFin = ''
                sectionRecord.detalleServicios = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200200/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200201/:
                                TxtDigester.SectionRecord tmpSectionRecord = new TxtDigester.SectionRecord()
                                tmpSectionRecord.codigo = line[7..-1].trim()
                                sectionRecord.detalleServicios << tmpSectionRecord
                                break
                            case ~/^200202/:
                                sectionRecord.detalleServicios.last()['tel'] = line[7..-1].trim()
                                break
                            case ~/^200203/:
                                sectionRecord.detalleServicios.last()['razonSocial'] = line[7..-1].trim()
                                break
                            case ~/^200204/:
                                String lineV2 = line[7..-1].trim()
                                sectionRecord.detalleServicios.last()['servicioNombre'] = lineV2

                                // Extract dates between parenthesis
                                // SAMPLE: 2002044G-PLAN LTE S/CONTRATO 15GB AP(12Jun al 11Jul)
                                if ((lineV2 ==~ /.*\(\d{2}.{3} al \d{2}.{3}\)$/) && (sectionRecord.detalleServicios.size() == 1)) {
                                    int idx1 = lineV2.lastIndexOf('(')
                                    int idx2 = lineV2.lastIndexOf(')')
                                    String dates = lineV2[idx1+1..idx2-1]
                                    List dateTokens = dates.tokenize()
                                    sectionRecord.periodoInicio = dateTokens[0]
                                    sectionRecord.periodoFin = dateTokens[2]
                                }

                                sectionRecord.detalleServicios.last()['servicios'] = []
                                break
                            case ~/^200205/:
                                // PLANES CONTRATADOS
                                // PLAN DE VOZ
                                // CARGOS ADICIONALES
                                // ET AL
                                def tokens = line[7..-1].split(' Q ')
                                // --
                                TxtDigester.SectionRecord tmpSubSectionRecord = new TxtDigester.SectionRecord()
                                tmpSubSectionRecord.servicioNombre = tokens[0].trim()
                                tmpSubSectionRecord.servicioTotal = FormatUtil.currencyToNumberFormatter(tokens[1])
                                tmpSubSectionRecord.servicioItems = [] // new List
                                // --
                                sectionRecord.detalleServicios.last()['servicios'] << tmpSubSectionRecord
                                break
                            case ~/^200206/:
                                def tokens = line[7..-1].split(' Q ')
                                TxtDigester.SectionRecord itemSubSectionRecord = new TxtDigester.SectionRecord()
                                itemSubSectionRecord.servicioItemNombre = tokens[0].trim()
                                if (tokens.length > 1) {
                                    itemSubSectionRecord.servicioItemValor = FormatUtil.currencyToNumberFormatter(tokens[1])
                                }
                                sectionRecord.detalleServicios.last()['servicios'].last()['servicioItems'] << itemSubSectionRecord
                                break
                            case ~/^200207/:
                                def tokens = line[7..-1].split(' Q ')
                                sectionRecord.detalleServicios.last()['totalPagar'] = FormatUtil.currencyToNumberFormatter(tokens[1])
                                //sectionRecord.totalPagar = FormatUtil.currencyFormatter(tokens[1])
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
        '200300' : [ // DetalleServiciosAgrupados
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.detalleServiciosAgrupados = [] // new List
                TxtDigester.SectionRecord telefonoSectionRecord
                //TxtDigester.SectionRecord planSectionRecord
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH) // NOTE: Special case
                    try {
                        switch (id) {
                            case ~/^200300/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200301/:
                                telefonoSectionRecord = new TxtDigester.SectionRecord()
                                telefonoSectionRecord.codigoCliente = line[7..-1].trim()
                                sectionRecord.detalleServiciosAgrupados << telefonoSectionRecord
                                break
                            case ~/^200302/:
                                telefonoSectionRecord.telefono = line[7..-1].trim() // ****
                                break
                            case ~/^200303/:
                                telefonoSectionRecord.razonSocial = line[7..-1].trim()
                                break
                            case ~/^200304/:
                                telefonoSectionRecord.contrato = line[7..-1].trim()
                                telefonoSectionRecord.planesGlobales = [] // new List !!!!!!!!!!!!!!
                                break
                            case ~/^200305/: // Planes
                                def tokens = line[7..-1].split(' Q ')
                                TxtDigester.SectionRecord planesSectionRecord = new TxtDigester.SectionRecord()
                                planesSectionRecord.planGlobalNombre = tokens[0].trim()
                                planesSectionRecord.planGlobalTotal = FormatUtil.currencyToNumberFormatter(tokens[1])
                                planesSectionRecord.planes = [] // new List !!!!!!!!!!!!!!
                                // --
                                telefonoSectionRecord.planesGlobales << planesSectionRecord
                                break
                            case ~/^200306/: // Plan
                                if (line.take(LINE_ID_LENGTH + 2) ==~ /^200306[67]\w+/) { // NOT EMPTY
                                    def tokens = line[7..-1].split(' Q ')
                                    TxtDigester.SectionRecord planSectionRecord = new TxtDigester.SectionRecord()
                                    planSectionRecord.planId = line[6..<7]
                                    planSectionRecord.planNombre = FormatUtil.currencyToNumberFormatter(tokens[0])
                                    planSectionRecord.planTotal = FormatUtil.currencyToNumberFormatter(tokens[1])
                                    planSectionRecord.subPlanes = [] // new List !!!!!!!!!!!!!!
                                    // --
                                    telefonoSectionRecord.planesGlobales.last().planes << planSectionRecord
                                }
                                break
                            case ~/^200307/:
                                def tokens = line[7..-1].split(' Q ')
                                TxtDigester.SectionRecord subPlanSectionRecord = new TxtDigester.SectionRecord()
                                subPlanSectionRecord.subPlanNombre = FormatUtil.currencyToNumberFormatter(tokens[0])
                                if (tokens.length > 1) {
                                    subPlanSectionRecord.subPlanTotal = FormatUtil.currencyToNumberFormatter(tokens[1])
                                }

                                try {
                                    telefonoSectionRecord.planesGlobales.last().planes.last().subPlanes << subPlanSectionRecord
                                } catch(Exception e) {
                                    // It did not pass by 200306, therefore invent a new Plan
                                    TxtDigester.SectionRecord planSectionRecord = new TxtDigester.SectionRecord()
                                    planSectionRecord.planId = ''
                                    planSectionRecord.planNombre = ''
                                    planSectionRecord.planTotal = ''
                                    planSectionRecord.subPlanes = [subPlanSectionRecord] // new List with initial item!!!!!!!!!!!!!!
                                    // --
                                    telefonoSectionRecord.planesGlobales.last().planes << planSectionRecord
                                }
                                break
                            case ~/^200308/:
                                telefonoSectionRecord.totalPagar = FormatUtil.currencyToNumberFormatter(line[106..-1])
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
        '200400' : [ // DetalleDeConectividadEmpresarial
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200400/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
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
        '200500' : [ // DetalleFinanciamientos
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.detalleFinanciamientos = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200500/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200503/:
//                                String lineV2 = line[7..-1]
//                                List<String> tokens = []
//                                def dynamicDelimiter = '-\\s+\\d{1,2}/\\d{1,2}\\s+Q' // sample: '-  1/12    Q'
//                                String delimiter
//                                String _delim
//                                while (_delim = lineV2.find(dynamicDelimiter)) {
//                                    tokens << lineV2.substring(0, lineV2.indexOf(_delim))
//                                    lineV2 = lineV2.substring(lineV2.indexOf(_delim) + _delim.length())
//                                    delimiter = _delim
//                                }
//                                tokens << lineV2
//                                if (tokens.size() != 2) {
//                                    throw new Exception("Cantidad de tokens no esperada: ${tokens.size()}")
//                                }
                                //def cuotaTokens = tokens[1].tokenize('Q')
                                def cuotaTokens = line[106..-1].tokenize('Q')
                                if (cuotaTokens.size() != 3) {
                                    throw new Exception("Cantidad de tokens no esperada: ${cuotaTokens.size()}")
                                }
                                // --
                                TxtDigester.SectionRecord financiamientoSectionRecord = new TxtDigester.SectionRecord()
                                //financiamientoSectionRecord.articulo = tokens[0].trim()
                                def lineP1 = line[7..94]
                                def tokens = lineP1.tokenize()
                                financiamientoSectionRecord.ordenCompra = tokens[0]
                                financiamientoSectionRecord.telefono = tokens[1]
                                financiamientoSectionRecord.articulo = lineP1.replace(tokens[0], '').replace(tokens[1], '').trim()
                                //financiamientoSectionRecord.cuotaNum = delimiter.replace('-', '').replace('Q', '').trim()
                                financiamientoSectionRecord.cuotaNum = line[97..102].trim()
                                financiamientoSectionRecord.cuotaMes = FormatUtil.currencyToNumberFormatter(cuotaTokens[0])
                                financiamientoSectionRecord.pendientePagar = FormatUtil.currencyToNumberFormatter(cuotaTokens[1])
                                financiamientoSectionRecord.totalFinanciado = FormatUtil.currencyToNumberFormatter(cuotaTokens[2])
                                sectionRecord.detalleFinanciamientos << financiamientoSectionRecord
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
        '200600' : [ // DetalleConveniosPagos
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.detalleConveniosPagos = [] // new List
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^200600/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^200603/:
                                def lineV2 = line[7..-1]
                                def tokens = lineV2.split(' - ')
                                def cuotaTokens = tokens[1].tokenize('Q')
                                // --
                                TxtDigester.SectionRecord convenioSectionRecord = new TxtDigester.SectionRecord()
                                convenioSectionRecord.convenio = tokens[0].trim()
                                convenioSectionRecord.pagoNum = cuotaTokens[0].trim()
                                convenioSectionRecord.pagoMes = FormatUtil.currencyToNumberFormatter(cuotaTokens[1])
                                convenioSectionRecord.pendientePagar = FormatUtil.currencyToNumberFormatter(cuotaTokens[2])
                                convenioSectionRecord.totalPagar = FormatUtil.currencyToNumberFormatter(cuotaTokens[3])
                                sectionRecord.detalleConveniosPagos << convenioSectionRecord
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
        '300000' : [ // Detalle Llamadas (Consumos)
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.detalleConsumos = []
                String detalleServiciosDynamicKey
                String detalleConsumosDynamicKey
                String detalleConsumosName // To overwrite ConsumoType if needed
                def none = false
                //int tokensQty = 0
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^300000/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^300100/:
                                TxtDigester.SectionRecord tmpSectionRecord = new TxtDigester.SectionRecord()
                                tmpSectionRecord['telefono'] = line[34..-1].trim()
                                sectionRecord.detalleConsumos << tmpSectionRecord
                                break
                            case ~/^300101/:
                                // DETALLE DE USO DE LOS SERVICIOS INCLUIDOS DENTRO DEL PLAN CONTRATADO
                                // DETALLE DE USO DE LOS SERVICIOS EXCEDENTES
                                // DETALLE DE SERVICIOS DE VALOR AGREGADO
                                def str = line[7..-1]
                                def order = TxtSpecs.OrderType.forKeyword(str).getOrder()
                                detalleServiciosDynamicKey = "${order}_${str.tokenize().join('_').toLowerCase()}"
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey] = new TxtDigester.SectionRecord()
                                break
                            case ~/^300102/:
                                detalleConsumosName = line[7..-1]
                                //println detalleConsumosName
                                none = detalleConsumosName == 'NONE'
                                // --
                                if (!line.contains(' ')) { // One single WORD
                                    line = "${line}_" // Add UNDERSCORE to recognize as dynamic key
                                }
                                def str = line[7..-1]
                                def order = TxtSpecs.SubOrderType.forKeyword(str).getOrder()
                                detalleConsumosDynamicKey = "${order}_${str.tokenize().join('_').toLowerCase()}"
                                //detalleConsumosDynamicKey = line[7..-1].tokenize().join('_').toLowerCase()
                                // --
                                TxtDigester.SectionRecord tmpSectionRecord = new TxtDigester.SectionRecord()
                                tmpSectionRecord.items = [] // new List
                                // --
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey] = tmpSectionRecord
                                break
                            case ~/^300103/:
                                // TODO: Temporal block of code
                                // Rare case
                                // --NONE--
                                // * 1.NONE
                                if (none) {
                                    sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].consumoType = ConsumoType.MENSAJE //NONE
                                    //sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].cabeceras = ['NONE', 'NONE', 'NONE', 'NONE', 'NONE', 'NONE']
                                    sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].cabeceras = ['FECHA', 'HORA', 'DESTINO', 'LUGAR', 'MENSAJE', 'VALOR']
                                    sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].columnas = 1
                                    break
                                }

                                // --DETALLE DE LLAMADAS SALIENTES LOCAL--
                                // * 1.FECHA      2.HORA         3.DESTINO                   4.LUGAR        5.MM:SS/LLAM    6.VALOR
                                // or
                                // --DETALLE DE MENSAJES DE TEXTO LOCAL--
                                // * 1.FECHA      2.HORA         3.DESTINO                   4.LUGAR       5.MENSAJE    6.VALOR
                                // or
                                // --DETALLE DE NAVEGACIÓN LOCAL--
                                // * 1.FECHA      2.HORA         3.DESCRIPCION            4.KILOBYTES TRANSMITIDOS   5.VALOR
                                // or
                                // --DETALLE DE RECARGAS PROGRAMADAS POR EVENTO--
                                // * 1.FECHA      2.HORA         3.DESTINO             4.DESCRIPCION       5.EVENTO     6.VALOR
                                // or
                                // --CLARO ENTRETENIMIENTO--
                                // * 1.FECHA      2.HORA           3.DESCRIPCION                         4.EVENTO     5.VALOR
                                // or
                                //--PRODUCTIVIDAD--
                                // * 1.FECHA      2.HORA           3.DESCRIPCION                         4.EVENTO     5.VALOR

                                // Calc quantity of tokens dynamically
                                List cabeceras = []
                                List headers = line[7..-1].split(/\s{2}/).findAll()
                                headers.each {
                                    if (!cabeceras.contains(it.trim())) {
                                        cabeceras.add(it.trim())
                                    }
                                }
                                int columnas = headers.size() / cabeceras.size()
                                int tokensQty = cabeceras.size()
                                if (tokensQty < 5 && tokensQty > 6) {
                                    throw new Exception("Cantidad de tokens no esperada: ${tokensQty}")
                                }
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].cabeceras = cabeceras
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].columnas = columnas

                                ConsumoType consumoType = ConsumoType.forDesc(cabeceras[-2])
//                                if (consumoType == ConsumoType.EVENTO) {
//                                    println consumoType
//                                }
                                if (consumoType != ConsumoType.LLAMADA && detalleConsumosName.contains('LLAMADAS')) {
                                    // Overwrite
                                    consumoType = ConsumoType.LLAMADA
                                }
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].consumoType = consumoType
                                break
                            case ~/^300104/:
                                //if (none){
                                //    break
                                //}

                                // SAMPLES:
                                // 300104404/01/2021 08:08:15 5023249-4039        Guatemala     5259625      1        0.00 | 04/01/2021 08:08:35 5024647-1422        Comcel +50246 5260398      1        0.00
                                // 300104427/01/2021 11:06:08 5027823-3944        Telgua +5027823             2:00        0.00 |
                                // ROAMING:
                                // 300104431/12/2020 08:57:24 5025110-4939        El Salvador-Claro Guatemala                                                                        4:00        0.00
                                // 300104407/03/2021 13:38:49 Claromusica susc Mes                           1       54.80 |
                                // 300104427/02/2021 04:43:47 SVAYTECNO SUSC 70700                           1       11.55 | 06/03/2021 04:43:32 SVAYTECNO SUSC 70700                           1       11.55
                                String lineV2 = line[7..-1].replaceAll('\\s{2,}', ' ')

                                if (!lineV2.contains('|')) {
                                    lineV2 += ' | '
                                }

                                int expectedTokensSize = sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].cabeceras.size()

                                List subLines = lineV2.tokenize('|')
                                subLines.each { subLine ->
                                    if (subLine.trim().isEmpty()) {
                                        return
                                    }

                                    List tokens = subLine.trim().tokenize()

                                    // Token 1 (valor)
                                    String valor = FormatUtil.currencyToNumberFormatter(tokens.pop()) // Take and remove last element. Sample: 0.00
                                    if (!(valor ==~ /\d+\.\d{2}/)) {
                                        throw new Exception("Invalid value for 'valor': ${valor}")
                                    }

                                    // Token 2 (cantidad)
                                    String cantidad = tokens.pop() // Take and remove last element. Sample: 1 | 2:00
                                    if (!(cantidad ==~ /\d+(:\d{2})?/)) {
                                        throw new Exception("Invalid value for 'cantidad': ${valor}")
                                    }

                                    // Token 3 (fecha)
                                    String fecha = tokens.first() // Take first element. Sample: 04/01/2021
                                    tokens.remove(0) // Remove first element
                                    if (!(fecha ==~ /\d{2}\/\d{2}\/\d{4}/)) {
                                        throw new Exception("Invalid value for 'fecha': ${valor}")
                                    }

                                    // Token 4 (hora)
                                    String hora = tokens.first() // Take first element. 08:08:15
                                    tokens.remove(0) // Remove first element
                                    if (!(hora ==~ /\d{2}:\d{2}:\d{2}/)) {
                                        throw new Exception("Invalid value for 'hora': ${valor}")
                                    }

                                    // Token 5 (destino|descripcion)
                                    String des
                                    if (expectedTokensSize == 5) {
                                        des = tokens.join(' ') // Join remaining elements
                                    } else { expectedTokensSize == 6
                                        des = tokens.first() // Take first element
                                        if (detalleConsumosDynamicKey.toUpperCase().contains('ROAMING') && des == 'Llamada') {
                                            des = ''
                                        } else {
                                            tokens.remove(0) // Remove first element
                                        }
                                    }

                                    // Token 6 [opcional]
                                    String lugar
                                    if (expectedTokensSize == 6) {
                                        // Telefonica +50243 -> Otros Operadores
                                        lugar = tokens.join(' ') // Join remaining elements
                                        for (int i = 0; i < TABLES.Carrier.size(); i++) {
                                            if (lugar.contains(TABLES.Carrier[i])) {
                                                lugar = lugar.replace(TABLES.Carrier[i], 'Otros Operadores')
                                                lugar = lugar.replace('+', '')
                                                lugar = lugar.replaceAll('\\d', '')
                                                lugar = lugar.trim()
                                                break
                                            }
                                        }
                                    }

                                    // Object
                                    TxtDigester.SectionRecord consumoSectionRecord = new TxtDigester.SectionRecord()
                                    consumoSectionRecord.fecha = fecha
                                    consumoSectionRecord.hora = hora
                                    consumoSectionRecord.des = des // destino|descripcion
                                    consumoSectionRecord.lugar = lugar
                                    consumoSectionRecord.cantidad = cantidad
                                    consumoSectionRecord.valor = valor.toBigDecimal()
                                    // --
                                    sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].items << consumoSectionRecord
                                }
                                break
                            case ~/^300105/:
                                //if (none){
                                    //break
                                //}

                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].cantidadTotal = line[32..45].trim()
                                sectionRecord.detalleConsumos.last()[detalleServiciosDynamicKey][detalleConsumosDynamicKey].valorTotal = FormatUtil.currencyToNumberFormatter(line[46..-1].trim()).toBigDecimal()
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
        '400000' : [ // Ajustes
            parse : { List<String> lines ->
                TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord()
                sectionRecord.ajustes = [] // new List
                TxtDigester.SectionRecord ajusteSectionRecord
                lines.each { line ->
                    String id = line.take(LINE_ID_LENGTH)
                    try {
                        switch (id) {
                            case ~/^400000/:
                                sectionRecord.id = id
                                sectionRecord.seccion = line[34..-1].trim()
                                break
                            case ~/^400001/:
                                def tokens = line[7..-1].tokenize()
                                //sectionRecord.datoX = tokens[1]
                                //sectionRecord.preImpreso = tokens[1]
                                break
                            case ~/^400002/:
                                ajusteSectionRecord = new TxtDigester.SectionRecord()
                                sectionRecord.ajustes << ajusteSectionRecord
                                // --
                                def serie = line[7..14]
                                def numero = line[15..38].trim()
                                def fechaEmision = line[45..54]
                                def nombre = line[55..254].trim()
                                def direccion = line[255..454].trim()
                                def nit = line[455..474].trim()
                                def numeroAutorizacion = line[475..-1]
                                // --
                                ajusteSectionRecord.serie = serie
                                ajusteSectionRecord.numero = numero
                                ajusteSectionRecord.fechaEmision = fechaEmision
                                ajusteSectionRecord.nombre = nombre
                                ajusteSectionRecord.direccion = direccion
                                ajusteSectionRecord.nit = nit
                                ajusteSectionRecord.numeroAutorizacion = numeroAutorizacion
                                break
                            case ~/^400003/:
                                def tokens = line[7..-1].tokenize()
                                ajusteSectionRecord.cuenta = tokens[0]
                                ajusteSectionRecord.fechaFactura = tokens[1].take(10)
                                ajusteSectionRecord.serieFactura = tokens[1][10..-1]
                                ajusteSectionRecord.numeroFactura = tokens[2]
                                ajusteSectionRecord.numeroServicio = tokens[3].tokenize('Q')[0]
                                ajusteSectionRecord.importeFactura = FormatUtil.currencyToNumberFormatter(tokens[3].tokenize('Q')[1])
                                break
                            case ~/^400004/:
                                ajusteSectionRecord.numCuenta = line[7..30].trim()
                                ajusteSectionRecord.concepto = line[31..60].trim()
                                ajusteSectionRecord.valor = FormatUtil.currencyToNumberFormatter(line[61..74])
                                ajusteSectionRecord.total = FormatUtil.currencyToNumberFormatter(line[75..88])
                                ajusteSectionRecord.totalLetras = line[89..-1]
                                break
                            case ~/^400005/:
                                def tokens = line[7..-1].split('NIT:')
                                ajusteSectionRecord.facturador = tokens[0].trim()
                                ajusteSectionRecord.facturadorNit = tokens[1].trim()
                                break
                            case ~/^400006/:
                                ajusteSectionRecord.tipo = line[7..-1]
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
        '900000' : [
                parse : { List<String> lines /*, String telefonoTitular*/ ->
                    TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord(id: lines[0].take(LINE_ID_LENGTH))
                    return sectionRecord
                }
        ],
        '999999' : [
                parse : { List<String> lines /*, String telefonoTitular*/ ->
                    TxtDigester.SectionRecord sectionRecord = new TxtDigester.SectionRecord(id: lines[0].take(LINE_ID_LENGTH))
                    return sectionRecord
                }
        ],
    ]

    static final TABLES = [
        Ciclo_XT: [
            '01', '07', '10', '11', '21', '77', '78'
        ],
        Carrier : [
            'Bell South',
            'Comcel',
            'Telefonica',
            'UNITEL',
            'Atel',
            'ATT',
            'Guatel',
            'Telefonos del N',
            'Telefonos del Norte',
        ]
    ]
}



class TxtDigester {
    private static String SECTION_DELIMITER = '=' * 28

    private static Map txtSections
    //private static Map datTables

    TxtDigester(Map txtSections/*, Map datTables*/) {
        this.txtSections = txtSections
        //this.datTables = datTables
    }

    static class MultisectionRecord extends TreeMap {
        static List itemsClassifiedByConsumptionType(SectionRecord detalle, TxtSpecs.ConsumoType consumoType) {
            def list = [] // new List
            LinkedHashSet dynamicOuterKeys = detalle.keySet().findAll({it.contains('_')})
            dynamicOuterKeys.forEach { String key1 ->
                LinkedHashSet dynamicInnerKeys = detalle[key1].keySet().findAll({it.contains('_')})
                dynamicInnerKeys.forEach { String key2 ->
                    if (detalle[key1][key2].consumoType == consumoType) {
                        list << detalle[key1][key2].items
                    }
                }
            }
            return list.flatten().findAll()
        }

        static String quantitySummationClassifiedByConsumptionType(SectionRecord detalle, TxtSpecs.ConsumoType consumoType) {
            // -- VOICE:
            int minutes = 0
            int seconds = 0
            // -- ET AL:
            int quantity = 0

            LinkedHashSet dynamicOuterKeys = detalle.keySet().findAll({it.contains('_')})
            dynamicOuterKeys.forEach { String key1 ->
                LinkedHashSet dynamicInnerKeys = detalle[key1].keySet().findAll({it.contains('_')})
                dynamicInnerKeys.forEach { String key2 ->
                    if (detalle[key1][key2].consumoType == consumoType) {
                        if (detalle[key1][key2].containsKey('cantidadTotal')) {
                            if (detalle[key1][key2].cantidadTotal.contains(':')) {
                                def tokens = detalle[key1][key2].cantidadTotal.split(':')
                                minutes += tokens[0].toInteger()
                                seconds += tokens[1].toInteger()
                            } else {
                                quantity += detalle[key1][key2].cantidadTotal.toInteger()
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

        static BigDecimal valueSummationClassifiedByConsumptionType(SectionRecord detalle, TxtSpecs.ConsumoType consumoType) {
            BigDecimal summation = new BigDecimal('0.00')
            LinkedHashSet dynamicOuterKeys = detalle.keySet().findAll({it.contains('_')})
            dynamicOuterKeys.forEach { String key1 ->
                LinkedHashSet dynamicInnerKeys = detalle[key1].keySet().findAll({it.contains('_')})
                dynamicInnerKeys.forEach { String key2 ->
                    if (detalle[key1][key2].consumoType == consumoType) {
                        if (detalle[key1][key2].containsKey('valorTotal')) {
                            summation += detalle[key1][key2].valorTotal
                        }
                    }
                }
            }
            //println("SUMMATION ${consumoType}:  ${summation}")
            return summation
        }
    }

    static class SectionRecord extends TreeMap {
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

        //String telefonoTitular = multisectionRecord['01000']?.telefono ?: '???'

        try {
            SectionRecord sectionRecord = txtSections[sectionType.id].parse(sectionLines)
            multisectionRecord[sectionType.id] = sectionRecord
        } catch(Exception ex) {
            println "EXCEPTION on line: ${Globals.LINE_ERROR}"

//            def lock = new Object()
//            synchronized(lock) {
//                new File('.','movil.err').withWriter('utf-8') {
//                    //writer -> writer.writeLine "EXCEPTION on line: ${Globals.LINE_ERROR}"
//                    writer -> writer.append "EXCEPTION on line: ${Globals.LINE_ERROR}\n"
//                }
//            }

            throw ex
        }

        //println "lineRecord(${lineRecord.size()}): ${lineRecord}"

        //println "***populateMultilineRecord closure took ${TimeCategory.minus(new Date(), start)}"
        return true
    }

    private static void postPopulateMultisectionRecord(MultisectionRecord multisectionRecord) {
        altSubPlanes(multisectionRecord)
        altPeriodo(multisectionRecord)
    }

    private static void altSubPlanes(MultisectionRecord multisectionRecord) {
        multisectionRecord['200300'].detalleServiciosAgrupados.each { TxtDigester.SectionRecord sectionRecord ->
            sectionRecord.planesGlobales.each { TxtDigester.SectionRecord planGlobalItem ->
                planGlobalItem.planes.each { TxtDigester.SectionRecord planItem ->
                    if (planItem.subPlanes.size() == 0) {
                        TxtDigester.SectionRecord subPlanSectionRecord = new TxtDigester.SectionRecord()
                        subPlanSectionRecord.subPlanNombre = ''
                        subPlanSectionRecord.subPlanTotal = ''
                        planItem.subPlanes << subPlanSectionRecord
                    } else {
                        planItem.subPlanes.each { TxtDigester.SectionRecord subPlanItem ->
                            if (!subPlanItem.containsKey('subPlanTotal')) {
                                subPlanItem.subPlanTotal = ''
                            }
                        }
                    }
                }
            }
        }
    }

    private static void altPeriodo(MultisectionRecord multisectionRecord) {
        //Para los clientes con ImprimirPeriodo=S hay que tomar el periodo de la sección DetalleServicios del canal 4, al final de la linea debe estar el periodo entre paréntesis
        if (multisectionRecord['100400'].imprimirPeriodo == 'S') {
            //&& !multisectionRecord['200200'].periodoInicio.isEmpty() && !multisectionRecord['200200'].periodoFin

            def (String periodoInicio, String periodoFin) = FormatUtil.dateFormatter_ddMMM_TO_SeparatedBySlash_dd_MM_yyyy(multisectionRecord['200200'].periodoInicio, multisectionRecord['200200'].periodoFin, multisectionRecord['100000'].fechaEmision)
            multisectionRecord['100000'].periodoInicio = periodoInicio
            multisectionRecord['100000'].periodoFin = periodoFin
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
        //TxtSpecs.SectionType sectionType
        
        lines.each { line ->
            String lineId = line.take(TxtSpecs.LINE_ID_LENGTH)
            boolean startsNewSection = TxtSpecs.SectionType.startsNewSectionExcludingVeryFirstSection(lineId)
            TxtSpecs.SectionType sectionType = TxtSpecs.SectionType.forId(lineId)
            boolean endOfMultilineSection = TxtSpecs.SectionType.endOfMultilineSection(sectionType)
            
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
        def clienteCodigoV2 = multisectionRecord['100200'].clienteCodigo.replaceAll('\\.', '')
        return CryptoUtil.calculateMD5(clienteCodigoV2 + multisectionRecord['100000'].fechaEmision)
    }

}



class XmlUtil {

    static String buildXml(TxtDigester.MultisectionRecord multisectionRecord) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.setDoubleQuotes(true)

        def clienteCodigoV2 = multisectionRecord['100200'].clienteCodigo.replaceAll('\\.', '')

        // MarkupBuilder gives us an instance of MarkupBuilderHelper named 'mkp'
        // MarkupBuilderHelper has several helpful methods
//        xml.mkp.xmlDeclaration(version: "1.0", encoding: "utf-8")
//        xml.'ubf:bill'('version': 1) { // ROOT TAG
        xml.BILL {
            'ubf:info' { // TAG
                'ubf:billId' "C${clienteCodigoV2}C${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_yyyyMMdd(multisectionRecord['100000'].fechaEmision)}" // TAG
                'ubf:internalAccountNo' "C${clienteCodigoV2}C" // TAG
                'ubf:externalAccountNo' "${clienteCodigoV2}" // TAG
                'ubf:billDate' "${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(multisectionRecord['100000'].fechaEmision)}" // TAG
                'ubf:dueDate' "${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(multisectionRecord['100000'].fechaLimitePago)}" // TAG
                'ubf:finalBill' false // TAG
                'ubf:accountClosed' false // TAG
                'ubf:disablePayment' false // TAG
                'ubf:amountDue' "${multisectionRecord['100300'].saldoMora}" // TAG
                'ubf:billAmount' "${multisectionRecord['100300'].totalFinal}" // TAG
                'ubf:minimumDue' "${multisectionRecord['100300'].totalFinal}" // TAG
                'ubf:overdue' false // TAG
                'ubf:billingAddress' { // TAG
                    'ubf:line' "${multisectionRecord['100200'].clienteDireccionL1}" // TAG
                    'ubf:line' "${multisectionRecord['100200'].clienteDireccionL2}" // TAG
                    'ubf:line' "${multisectionRecord['100200'].clienteDireccionL3}" // TAG
                } // ubf:billingAddress
                'ubf:orgId' 'ORG_ID' //"${clienteCodigoV2}" // TAG
                'ubf:costCenter' 'COST_CENTER' //"${multisectionRecord['100200'].clienteNumeroTributario}" // TAG
                'ubf:selfReg0' "${multisectionRecord['100000'].documentoNumero}" // TAG
                'ubf:selfReg1' "${multisectionRecord['100200'].clienteNumeroTributario}"// TAG
                'ubf:selfReg2' 'GT' // TAG
                'ubf:selfReg3' "VIDEO_URL/claro/embed-iframe.php?uid=${TxtDigester.calcUidMD5(multisectionRecord)}" // TAG // TODO: dataMap.uidMD5
                'ubf:selfReg4' "${multisectionRecord['100000'].preImpreso}" // TAG
            }
            'ubf:summary-xml'(layout: 'telco.demo', version: 1) { // TAG
                summary { // TAG
                    date(FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_yyyyMMdd(multisectionRecord['100000'].fechaEmision)) // TAG
                } // summary
                encabezado { // TAG
                    documentoTipo "${multisectionRecord['100000'].documentoTipo}" // TAG
                    documentoNumero "${multisectionRecord['100000'].documentoNumero}" // TAG
                    clienteCategoria "${multisectionRecord['100000'].clienteCategoria}" // TAG
                    clienteCiclo "${multisectionRecord['100000'].clienteCiclo}" // TAG
                    periodoInicio "${multisectionRecord['100000'].periodoInicio}" // TAG
                    periodoFin "${multisectionRecord['100000'].periodoFin}" // TAG
                    fechaAcreditacion "${multisectionRecord['100000'].fechaAcreditacion}" // TAG
                    fechaEmision "${multisectionRecord['100000'].fechaEmision}" // TAG
                    fechaLimitePago "${multisectionRecord['100000'].fechaLimitePago}" // TAG
                    noContratos "${multisectionRecord['100000'].noContratos}" // TAG
                    serie "${multisectionRecord['100000'].serie}" // TAG
                    preImpreso "${multisectionRecord['100000'].preImpreso}" // TAG
                    numeroAutorizacion "${multisectionRecord['100000'].numeroAutorizacion}" // TAG
                    serieAdministrativa "${multisectionRecord['100000'].serieAdministrativa}" // TAG
                    numeroAdministrativo "${multisectionRecord['100000'].numeroAdministrativo}" // TAG
                    detalleTrafico "${multisectionRecord['100000'].detalleTrafico}" // TAG
                } // encabezado
                emisor { // TAG
                    nombreEmisor "${multisectionRecord['100100'].nombreEmisor}" // TAG
                    numeroTributarioEmisor "${multisectionRecord['100100'].numeroTributarioEmisor}" // TAG
                    nombreComercialEmisor "${multisectionRecord['100100'].nombreComercialEmisor}" // TAG
                    direccionEmisor "${multisectionRecord['100100'].direccionEmisor}" // TAG
                    telefonoEmisor "${multisectionRecord['100100'].telefonoEmisor}" // TAG
                    faxEmisor "${multisectionRecord['100100'].faxEmisor}" // TAG
                    emailEmisor "${multisectionRecord['100100'].emailEmisor}" // TAG
                } // emisor
                receptor { // TAG
                    clienteNombre "${multisectionRecord['100200'].clienteNombre}"
                    clienteCodigo "${multisectionRecord['100200'].clienteCodigo}"
                    clienteId "${multisectionRecord['100200'].clienteId}"
                    clienteDireccionL1 "${multisectionRecord['100200'].clienteDireccionL1}"
                    clienteDireccionL2 "${multisectionRecord['100200'].clienteDireccionL2}"
                    clienteDireccionL3 "${multisectionRecord['100200'].clienteDireccionL3}"
                    clienteTelefono "${multisectionRecord['100200'].clienteTelefono}"
                    clienteEmail "${multisectionRecord['100200'].clienteEmail}"
                    clienteNumeroTributario "${multisectionRecord['100200'].clienteNumeroTributario}"
                } // receptor
                totales { // TAG
                    moneda "${multisectionRecord['100300'].moneda}" // TAG
                    simbolo "Q" // TAG
                    saldoCorteAnterior "${multisectionRecord['100300'].saldoCorteAnterior}" // TAG
                    totalAjustes "${multisectionRecord['100300'].totalAjustes}" // TAG
                    totalPagos "${multisectionRecord['100300'].totalPagos}" // TAG
                    saldoMora "${multisectionRecord['100300'].saldoMora}" // TAG
                    totalServicios "${multisectionRecord['100300'].totalServicios}" // TAG
                    totalImpuestoVenta "${multisectionRecord['100300'].totalImpuestoVenta}" // TAG
                    total "${multisectionRecord['100300'].total}" // TAG
                    totalExento "${multisectionRecord['100300'].totalExento}" // TAG
                    totalFinanciamiento "${multisectionRecord['100300'].totalFinanciamiento}" // TAG
                    totalConvenioPagos "${multisectionRecord['100300'].totalConvenioPagos}" // TAG
                    totalFinal "${multisectionRecord['100300'].totalFinal}" // TAG
                    totalLetras "${multisectionRecord['100300'].totalLetras}" // TAG
                } // totales
                personalizados { // TAG
                    codigoBarras "${multisectionRecord['100400'].codigoBarras}" // TAG
                    firmaElectronica "${multisectionRecord['100400'].firmaElectronica}" // TAG
                    courierCodigo "${multisectionRecord['100400'].courierCodigo}" // TAG
                    courierNombre "${multisectionRecord['100400'].courierNombre}" // TAG
                    courierRuta "${multisectionRecord['100400'].courierRuta}" // TAG
                    enviarCorreo "${multisectionRecord['100400'].enviarCorreo}" // TAG
                    imprimirPeriodo "${multisectionRecord['100400'].imprimirPeriodo}" // TAG
                    resolucion "${multisectionRecord['100400'].resolucion}" // TAG
                    sistemaEmisor "${multisectionRecord['100400'].sistemaEmisor}" // TAG
                    observaciones "${multisectionRecord['100400'].observaciones}" // TAG
                    correlativo "${multisectionRecord['100400'].correlativo}" // TAG
                } // personalizados
                mensajesMercadeo { // TAG
                    txt1 "${multisectionRecord['100500'].txt1}" // TAG
                    txt2 "${multisectionRecord['100500'].txt2}" // TAG
                    txt3 "${multisectionRecord['100500'].txt3}" // TAG
                } // mensajesMercadeo
                consolidados { // TAG // 100600 consolidados
                    multisectionRecord['100600'].consolidados.each { TxtDigester.SectionRecord sectionRecord ->
                        consolidado( // TAG
                                nombre: sectionRecord.nombre,
                                total: sectionRecord.total,
                                descuento: sectionRecord.descuento,
                                totalFinal: sectionRecord.totalFinal
                        )
                    }
                } // consolidados
                detalleSubCuentas(total: multisectionRecord['200000'].total) { // TAG
                    multisectionRecord['200000'].detalleSubCuentas.each {
                        subCuenta( // TAG
                                codigo: it.codigo,
                                nombre: it.nombre,
                                valor: it.valor
                        )
                    }
                } // detalleSubCuentas
                detalleCargosAdicionalesYPromociones { // TAG
                    multisectionRecord['200100'].detalleCargosAdicionalesYPromociones.each { TxtDigester.SectionRecord sectionRecord ->
                        detalleCargoAdicionalYPromocion( // TAG
                                codigo: sectionRecord.codigo,
                                nombre: sectionRecord.nombre,
                                totalPagar: sectionRecord.totalPagar) {
                            sectionRecord.cargosAdicionalesYPromociones.each { TxtDigester.SectionRecord sectionRecord1 ->
                                cargosAdicionalesYPromociones( // TAG
                                        nombre: sectionRecord1.cargoAdicionalYPromocionNombre,
                                        total: sectionRecord1.cargoAdicionalYPromocionTotal) {
                                    sectionRecord1.cargoAdicionalYPromocionItems.each { TxtDigester.SectionRecord item ->
                                        cargoAdicionalYPromocion ( // TAG
                                                nombre: item.cargoAdicionalYPromocionItemNombre,
                                                valor: item.cargoAdicionalYPromocionItemValor
                                        ) // cargoAdicionalYPromocion
                                    }
                                } // cargosAdicionalesYPromociones
                            }
                        } // detalleCargoAdicionalYPromocion
                    }
                } // detalleCargosAdicionalesYPromociones
                detalleServicios { // TAG
                    multisectionRecord['200200'].detalleServicios.each { TxtDigester.SectionRecord sectionRecord ->
                        detalleServicio(codigo: sectionRecord.codigo, tel: sectionRecord.tel, razonSocial: sectionRecord.razonSocial, servicioNombre: sectionRecord.servicioNombre, totalPagar: sectionRecord.totalPagar) { // TAG
                            sectionRecord.servicios.each { TxtDigester.SectionRecord sectionRecord2 ->
                                servicios(nombre: sectionRecord2.servicioNombre, valor: sectionRecord2.servicioTotal) { // TAG
                                    sectionRecord2.servicioItems.each { TxtDigester.SectionRecord sectionRecord3 ->
                                        servicio(nombre: sectionRecord3.servicioItemNombre, valor: sectionRecord3.servicioItemValor) // TAG
                                    }
                                }
                            }
                        } // detalleServicio
                    }
                } // detalleServicios
                detalleServiciosAgrupados { // TAG
                    multisectionRecord['200300'].detalleServiciosAgrupados.each { TxtDigester.SectionRecord sectionRecord ->
                        servicio( // TAG
                                codigoCliente: sectionRecord.codigoCliente,
                                telefono: sectionRecord.telefono,
                                razonSocial: sectionRecord.razonSocial,
                                contrato: sectionRecord.contrato,
                                planesContratadosTotal: sectionRecord.planesContratadosTotal,
                                totalPagar: sectionRecord.totalPagar
                        ) {
                            sectionRecord.planesGlobales.each { TxtDigester.SectionRecord planGlobalItem ->
                                planes(nombre: planGlobalItem.planGlobalNombre, total: planGlobalItem.planGlobalTotal) { // TAG
                                    planGlobalItem.planes.each { TxtDigester.SectionRecord planItem ->
                                        plan(id: planItem.planId, nombre: planItem.planNombre, total: planItem.planTotal) { // TAG
                                            subPlanes { // TAG
                                                planItem.subPlanes.each { TxtDigester.SectionRecord subPlanItem ->
                                                    // Extract id & nombre
                                                    subPlan(
                                                            nombre: subPlanItem.subPlanNombre,
                                                            total: subPlanItem.subPlanTotal
                                                    ) // TAG
                                                }
                                            } // subPlanes
                                        } // plan
                                    }
                                } // planes
                            }
                        } // detalleServicioAgrupado
                    }
                } // detalleServiciosAgrupados
                detalleDeConectividadEmpresarial { // TAG

                } // detalleDeConectividadEmpresarial
                detalleFinanciamientos { // TAG
                    multisectionRecord['200500'].detalleFinanciamientos.each { TxtDigester.SectionRecord financiamientoSectionRecord ->
                        financiamiento( // TAG
                                ordenCompra: financiamientoSectionRecord.ordenCompra,
                                telefono: financiamientoSectionRecord.telefono,
                                cuotaNum: financiamientoSectionRecord.cuotaNum,
                                cuotaMes: financiamientoSectionRecord.cuotaMes,
                                pendientePagar: financiamientoSectionRecord.pendientePagar,
                                totalFinanciado: financiamientoSectionRecord.totalFinanciado,
                                financiamientoSectionRecord.articulo
                        )
                    }
                } // detalleFinanciamientos
                detalleConveniosPagos { // TAG
                    multisectionRecord['200600'].detalleConveniosPagos.each { TxtDigester.SectionRecord convenioSectionRecord ->
                        convenio( // TAG
                                pagoNum: convenioSectionRecord.pagoNum,
                                pagoMes: convenioSectionRecord.pagoMes,
                                pendientePagar: convenioSectionRecord.pendientePagar,
                                totalPagar: convenioSectionRecord.totalPagar,
                                convenioSectionRecord.convenio
                        )
                    }
                } // detalleConveniosPagos
                ajustes {
                    multisectionRecord['400000'].ajustes.each {TxtDigester.SectionRecord item ->
                        ajuste(tipo: item.tipo) { // TAG
                            documentoTributarioElectronico { // TAG
                                serie("${item.serie}") // TAG
                                numero("${item.numero}") // TAG
                                fechaEmision("${item.fechaEmision}") // TAG
                                numeroAutorizacion("${item.numeroAutorizacion}")
                            } // documentoTributarioElectronico
                            cliente { // TAG
                                nombre("${item.nombre}") // TAG
                                direccion("${item.direccion}") // TAG
                                nit("${item.nit}") // TAG
                            } // cliente
                            datosFacturaAsociada { // TAG
                                cuenta("${item.cuenta}")
                                fechaFactura("${item.fechaFactura}")
                                serieFactura("${item.serieFactura}")
                                numeroFactura("${item.numeroFactura}")
                                numeroServicio("${item.numeroServicio}")
                                importeFactura("${item.importeFactura}")
                            } // datosFacturaAsociada
                            detalle { // TAG
                                numCuenta("${item.numCuenta}")
                                concepto("${item.concepto}")
                                valor("${item.valor}")
                                total("${item.total}")
                                totalLetras("${item.totalLetras}")
                            } // detalle
                            facturador { // TAG
                                nombre("${item.facturador}")
                                nit("${item.facturadorNit}")
                            }
                        } // ajuste
                    }
                } // ajustes
                history {} // Empty TAG
                historyGT {} // Empty TAG
                phoneUsage { // TAG
                    multisectionRecord['300000'].detalleConsumos.each { TxtDigester.SectionRecord detalleConsumo ->
    //                    List llamadas = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                        String sumatoriaLlamadasCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                        BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                        // --
    //                    List mensajes = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                        String sumatoriaMensajesCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                        BigDecimal sumatoriaMensajesValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                        // --
    //                    List datos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        String sumatoriaDatosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                        // --
    //                    List eventos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                        String sumatoriaEventosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                        BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                        // --
                        BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaMensajesValor + sumatoriaDatosValor
                        // --
                        phone(target: "${detalleConsumo.telefono.replace('-', '')}.phone") { // TAG
                            voice(minutes: sumatoriaLlamadasCantidad, cost: sumatoriaLlamadasValor, "${detalleConsumo.telefono.replace('-', '')}.calls") // TAG
                            data(usage: "[DATA_USAGE.${detalleConsumo.telefono.replace('-', '')}]", cost: 0.00, "${detalleConsumo.telefono.replace('-', '')}.data") // TAG
                            text(count: sumatoriaMensajesCantidad, cost: sumatoriaMensajesValor, "${detalleConsumo.telefono.replace('-', '')}.text") // TAG
                        } // phone
                    }
                } // phoneUsage
                phones { // TAG
                    multisectionRecord['300000'].detalleConsumos.each { TxtDigester.SectionRecord detalleConsumo ->
                        def nombre
                        def detalleServicio = multisectionRecord['200300'].detalleServiciosAgrupados.find({it.telefono == detalleConsumo.telefono.replace('-', '')})
                        if (detalleServicio != null) {
                            nombre = detalleServicio.razonSocial
                        } else {
                            nombre = multisectionRecord['100200'].clienteNombre
                        }
                        phone(id: detalleConsumo.telefono.replace('-', ''), target: "${detalleConsumo.telefono.replace('-', '')}.phone") { // TAG
                            summary { // TAG
                                name(nombre) // TAG
                                number(detalleConsumo.telefono.replace('-', ''))
                            } // summary
                        } // phone
                    }
                } // phones
            } // ubf:summary-xml
            // *****************************************************************************************************
            'ubf:children' {} // Empty TAG
        } // ubf:bill

        return writer.toString()
    }

    static boolean withChildren(TxtDigester.MultisectionRecord multisectionRecord) {
        boolean result = true
        if (TxtSpecs.TABLES.Ciclo_XT.contains(multisectionRecord['100000'].clienteCiclo) && multisectionRecord['100000'].detalleTrafico == 'N') {
            result = false
        }
        Globals.log.info("Document ${multisectionRecord['100000'].documentoNumero} ${result ? 'WILL' : 'WON\'T'} include children")
        return  result
    }

    static String buildChildrenDoc1(TxtDigester.MultisectionRecord multisectionRecord) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)
        xml.setDoubleQuotes(true)

        xml.'ubf:children' { // TAG
            multisectionRecord['300000'].detalleConsumos.each { TxtDigester.SectionRecord detalleConsumo ->
//                    List llamadas = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
//                    String sumatoriaLlamadasCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
//                    // --
//                    List mensajes = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    String sumatoriaMensajesCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                BigDecimal sumatoriaMensajesValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
//                    // --
//                    List datos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    String sumatoriaDatosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
//                    // --
//                    List eventos = multisectionRecord.itemsClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
//                    String sumatoriaEventosCantidad = multisectionRecord.quantitySummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                // --
                BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaMensajesValor + sumatoriaDatosValor
                // --
                'ubf:child'( // TAG
                        id: detalleConsumo.telefono,
                        linkid: "${detalleConsumo.telefono}.phone"
                ) {
                    'ubf:info' { // TAG
                        'ubf:name'multisectionRecord['100200'].clienteNombre
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
                        usageSummary(total: "${FormatUtil.NumberFormat_2dec.format(sumatoriaValorTotal)}") { // TAG
                            voiceCalls(label: 'Llamadas de Voz', FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)) // TAG
                            data(label: 'Uso de Datos', FormatUtil.NumberFormat_2dec.format(sumatoriaDatosValor)) // TAG
                            text(label: 'Mensajes de Texto', FormatUtil.NumberFormat_2dec.format(sumatoriaMensajesValor)) // TAG
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
                        'ubf:dataGroup'(id: detalleConsumo.telefono, linkId: detalleConsumo.telefono, layout: 'basic.call') { // TAG
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
                                servicios { // TAG
                                    LinkedHashSet serviceKeys = detalleConsumo.dynamicKeys()
                                    serviceKeys.each { String serviceKey ->
                                        def detalleServiciosNombre = serviceKey.tokenize('_').join(' ').toUpperCase()
                                        def order1 = detalleServiciosNombre[0..2] // Get order
                                        detalleServiciosNombre = detalleServiciosNombre[4..-1] // Remove order
                                        servicio(orden: order1, nombre: detalleServiciosNombre) { // TAG
                                            LinkedHashSet consumptionKeys = detalleConsumo[serviceKey].dynamicKeys()
                                            consumptionKeys.each { String consumptionKey ->
                                                def consumoNombre = consumptionKey.tokenize('_').join(' ').toUpperCase()
                                                def order2 = consumoNombre[0..2] // Get order
                                                consumoNombre = consumoNombre[4..-1] // Remove order
                                                //consumos(nombre: TxtSpecs.DetalleConsumosType."${consumptionKey.toUpperCase()}".desc) { // TAG
                                                consumos(orden: order2, nombre: consumoNombre) { // TAG
                                                    "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}s" ( // TAG llamadas|mensajes|datos|eventos
                                                            cabeceras: detalleConsumo[serviceKey][consumptionKey].cabeceras.join(', '),
                                                            columnas: detalleConsumo[serviceKey][consumptionKey].columnas,
                                                            cantidadTotal: detalleConsumo[serviceKey][consumptionKey].cantidadTotal ?: '0',
                                                            valorTotal: detalleConsumo[serviceKey][consumptionKey].valorTotal ?: '0.00'
                                                    ) {
                                                        detalleConsumo[serviceKey][consumptionKey].items.each { TxtDigester.SectionRecord item ->
                                                            "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}" ( // TAG
                                                                    fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                    hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                    des: item.des,
                                                                    lugar: "${item.lugar ?: ''}",
                                                                    cantidad: item.cantidad, // mm:ss
                                                                    valor: item.valor
                                                            ) // llamada|mensaje|dato|evento
                                                        }
                                                    } // llamadas|mensajes|datos|eventos
                                                } // consumos
                                            }
                                        } // servicio
                                    }
                                } // servicios
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
            multisectionRecord['300000'].detalleConsumos.each { TxtDigester.SectionRecord detalleConsumo ->
                BigDecimal sumatoriaLlamadasValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.LLAMADA)
                BigDecimal sumatoriaMensajesValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.MENSAJE)
                BigDecimal sumatoriaDatosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.DATO)
                BigDecimal sumatoriaEventosValor = multisectionRecord.valueSummationClassifiedByConsumptionType(detalleConsumo, TxtSpecs.ConsumoType.EVENTO)
                BigDecimal sumatoriaValorTotal = sumatoriaLlamadasValor + sumatoriaMensajesValor + sumatoriaDatosValor
                // --
                'ubf:child'( // TAG
                        id: detalleConsumo.telefono.replace('-', ''),
                        linkid: "${detalleConsumo.telefono.replace('-', '')}.phone"
                ) {
                    'ubf:info' { // TAG
                        'ubf:name'multisectionRecord['100200'].clienteNombre
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
                        usageSummary(total: "${FormatUtil.NumberFormat_2dec.format(sumatoriaValorTotal)}") { // TAG
                            voiceCalls(label: 'Llamadas de Voz', FormatUtil.NumberFormat_2dec.format(sumatoriaLlamadasValor)) // TAG
                            data(label: 'Uso de Datos', FormatUtil.NumberFormat_2dec.format(sumatoriaDatosValor)) // TAG
                            text(label: 'Mensajes de Texto', FormatUtil.NumberFormat_2dec.format(sumatoriaMensajesValor)) // TAG
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
                        'ubf:dataGroup'(id: "${detalleConsumo.telefono.replace('-', '')}_calls", linkId: "${detalleConsumo.telefono.replace('-', '')}_calls", layout: 'basic.call') { // TAG
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
                                LinkedHashSet serviceKeys = detalleConsumo.dynamicKeys()
                                serviceKeys.each { String serviceKey ->
                                    def detalleServiciosNombre = serviceKey.tokenize('_')[1..-1].join(' ').toUpperCase()
                                    servicio(nombre: detalleServiciosNombre) { // TAG
                                        LinkedHashSet consumptionKeys = detalleConsumo[serviceKey].dynamicKeys()
                                        consumptionKeys.each { String consumptionKey ->
                                            def consumoNombre = consumptionKey.tokenize('_')[1..-1].join(' ').toUpperCase()
                                            //consumos(nombre: TxtSpecs.DetalleConsumosType."${consumptionKey.toUpperCase()}".desc) { // TAG
                                            if (detalleConsumo[serviceKey][consumptionKey].consumoType == TxtSpecs.ConsumoType.LLAMADA) {
                                                consumos(nombre: consumoNombre) { // TAG
                                                    "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}s" ( // TAG [llamadas]|mensajes|datos|eventos
                                                            cabeceras: detalleConsumo[serviceKey][consumptionKey].cabeceras.join(', '),
                                                            columnas: detalleConsumo[serviceKey][consumptionKey].columnas,
                                                            cantidadTotal: detalleConsumo[serviceKey][consumptionKey].cantidadTotal,
                                                            valorTotal: detalleConsumo[serviceKey][consumptionKey].valorTotal
                                                    ) {
                                                        detalleConsumo[serviceKey][consumptionKey].items.each { TxtDigester.SectionRecord item ->
                                                            "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}" ( // TAG
                                                                    fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                    hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                    des: item.des,
                                                                    lugar: "${item.lugar ?: ''}",
                                                                    cantidad: item.cantidad, // mm:ss
                                                                    valor: item.valor
                                                            ) // [llamada]|mensaje|dato|evento
                                                        }
                                                    } // [llamadas]|mensajes|datos|eventos
                                                } // consumos
                                            } else if (detalleConsumo[serviceKey][consumptionKey].consumoType == TxtSpecs.ConsumoType.EVENTO) {
                                                consumos(nombre: consumoNombre) { // TAG
                                                    "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}s" ( // TAG llamadas|mensajes|datos|[eventos]
                                                            cabeceras: detalleConsumo[serviceKey][consumptionKey].cabeceras.join(', '),
                                                            columnas: detalleConsumo[serviceKey][consumptionKey].columnas,
                                                            cantidadTotal: detalleConsumo[serviceKey][consumptionKey].cantidadTotal,
                                                            valorTotal: detalleConsumo[serviceKey][consumptionKey].valorTotal
                                                    ) {
                                                        detalleConsumo[serviceKey][consumptionKey].items.each { TxtDigester.SectionRecord item ->
                                                            "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}" ( // TAG
                                                                    fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                    hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                    des: item.des,
                                                                    lugar: "${item.lugar ?: ''}",
                                                                    cantidad: item.cantidad, // mm:ss
                                                                    valor: item.valor
                                                            ) // llamada|mensaje|dato|[evento]
                                                        }
                                                    } // llamadas|mensajes|datos|[eventos]
                                                } // consumos
                                            }
                                        }
                                    } // servicio
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                        // ** [mensajeseventos ** //
                        'ubf:dataGroup'(id: "${detalleConsumo.telefono.replace('-', '')}_text", linkId: "${detalleConsumo.telefono.replace('-', '')}_text", layout: 'basic.call') { // TAG
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
                                LinkedHashSet serviceKeys = detalleConsumo.dynamicKeys()
                                serviceKeys.each { String serviceKey ->
                                    def detalleServiciosNombre = serviceKey.tokenize('_')[1..-1].join(' ').toUpperCase()
                                    servicio(nombre: detalleServiciosNombre) { // TAG
                                        LinkedHashSet consumptionKeys = detalleConsumo[serviceKey].dynamicKeys()
                                        consumptionKeys.each { String consumptionKey ->
                                            def consumoNombre = consumptionKey.tokenize('_')[1..-1].join(' ').toUpperCase()
                                            if (detalleConsumo[serviceKey][consumptionKey].consumoType == TxtSpecs.ConsumoType.MENSAJE) {
                                                consumos(nombre: consumoNombre) { // TAG
                                                    "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}s" ( // TAG llamadas|[mensajes]|datos|eventos
                                                            cabeceras: detalleConsumo[serviceKey][consumptionKey].cabeceras.join(', '),
                                                            columnas: detalleConsumo[serviceKey][consumptionKey].columnas,
                                                            cantidadTotal: detalleConsumo[serviceKey][consumptionKey].cantidadTotal,
                                                            valorTotal: detalleConsumo[serviceKey][consumptionKey].valorTotal
                                                    ) {
                                                        detalleConsumo[serviceKey][consumptionKey].items.each { TxtDigester.SectionRecord item ->
                                                            "${detalleConsumo[serviceKey][consumptionKey].consumoType.name().toLowerCase()}" ( // TAG
                                                                    fecha: item.fecha, //FormatUtil.dateFormatterSeparatedBySlash_dd_MM_TO_SeparatedByDash_yyyy_MM_dd(llamada.fecha, multisectionRecord['04000'].mesFacturacion),
                                                                    hora: item.hora, // timeFormatterSeparatedByColon_HH_mm_ss(llamada.hora),
                                                                    des: item.des,
                                                                    lugar: "${item.lugar ?: ''}",
                                                                    cantidad: item.cantidad, // mm:ss
                                                                    valor: item.valor
                                                            ) // llamada|[mensaje]|dato|evento
                                                        }
                                                    } // llamadas|[mensajes]|datos|eventos
                                                } // consumos
                                            }
                                        }
                                    } // servicio
                                }
                            } // ubf:rawContents
                            'ubf:header''' // TAG
                            'ubf:footer''' // TAG
                        } // ubf:dataGroup
                        // ** [datos] ** //
                        'ubf:dataGroup'(id: "${detalleConsumo.telefono.replace('-', '')}_data", linkId: "${detalleConsumo.telefono.replace('-', '')}_data", layout: 'basic.call') { // TAG
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
                                def detalleServiciosNombre = "CONSUMO DE DATOS"
                                servicio(nombre: detalleServiciosNombre) { // TAG
                                    mkp.yieldUnescaped("<![CDATA[${detalleConsumo.telefono.replace('-', '')}_data]]>")
                                } // servicio
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
                            writer << '<DOCUMENT>\n' +
                                    '<ubf:datafile xmlns:ubf="com/sorrisotech/saas/ubf/v1/UBF">\n' +
                                    '    <ubf:FileHeader>\n' +
                                    '        <ubf:billStream>B2C_VIEW</ubf:billStream>\n' +
                                    '        <ubf:paymentGroup>noop</ubf:paymentGroup>\n' +
                                    '    </ubf:FileHeader>\n' +
                                    '    <ubf:bill version="1">\n'
                        } else if (line.contains('<ubf:children />')) {
                                // Reads and Writes content of document-children.doc1
                                String childrenFilename = filePath.getFileName().toString().replaceFirst('[.][^.]+$', '')
                                childrenFilename = "${childrenFilename}-children.doc1"
                                Path childrenFilePath = absDirPath.resolve(childrenFilename)
                                if (Files.exists(childrenFilePath)) {
                                    childrenFilePath.withReader { childrenReader ->
                                        childrenReader.eachLine { childrenLine ->
                                            writer << "${childrenLine}\n"
                                        }
                                    }
                                } else {
                                    writer << "${line}\n"
                                }
                        } else if (line.contains('</BILL>')) {
                            writer << '</ubf:bill>\n' +
                                    '    <ubf:footer>\n' +
                                    '        <ubf:bills>100</ubf:bills>\n' +
                                    '        <ubf:amount>63866.63</ubf:amount>\n' +
                                    '        <ubf:assets>100</ubf:assets>\n' +
                                    '    </ubf:footer>\n' +
                                    '</ubf:datafile>\n' +
                                    '</DOCUMENT>\n'
                        } else {
                            writer << "${line}\n"
                        }
                    }
                }
            }
        }

        ubfFilePath << '</SPOOL>'

        println "UBF.XML file ${ubfFilePath} generated for Doc1"
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
                        } else if (line.contains('<ubf:children />')) {
                            // Reads and Writes content of document-children.smartbill
                            String childrenFilename = filePath.getFileName().toString().replaceFirst('[.][^.]+$', '')
                            childrenFilename = "${childrenFilename}-children.smartbill"
                            Path childrenFilePath = absDirPath.resolve(childrenFilename)
                            if (Files.exists(childrenFilePath)) {
                                childrenFilePath.withReader { childrenReader ->
                                    childrenReader.eachLine { childrenLine ->
                                        writer << "${childrenLine}\n"
                                    }
                                }
                            } else {
                                writer << "${line}\n"
                            }
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



class JedisUtil {
//    private Jedis jedis
//    private volatile static JedisHelper instance
//
//    private JedisHelper() {
//        try {
//            jedis = new Jedis('172.16.204.169', 12000)
//            jedis.auth('H9TDajj8Eytt')
//        } catch (Exception e) {
//            println e.getMessage()
//        }
//    }
//
//    static JedisHelper getInstance() {
//        if (instance == null) {
//            synchronized (JedisHelper.class) {
//                if (instance == null) {
//                    instance = new JedisHelper()
//                }
//            }
//        }
//        return instance;
//    }

    static String fetchEmail(clienteCodigo) {
        try {
            def jedis = new Jedis('172.16.204.169', 12000)
            jedis.auth('H9TDajj8Eytt')
            def email = jedis.get("doc-one-${clienteCodigo}") ?: ''
            return email.toLowerCase()
        } catch(Exception e) {
            println e.getMessage()
            return ''
        }
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
        ATTACHMENT("ATTACHMENT"),
        ORG_ID("ORG_ID"),
        IMPRIMIR_PERIODO("IMPRIMIR_PERIODO")

        String name

        CsvHeaderType(String name) {
            this.name = name;
        }

        static List headers() {
            values().collect({it.getName()})
        }
    }

    static String buildCsvRecord(TxtDigester.MultisectionRecord multisectionRecord) {
        def clienteCodigoV2 = multisectionRecord['100200'].clienteCodigo.replaceAll('\\.', '')
        def clienteCodigoMD5 = CryptoUtil.calculateMD5(clienteCodigoV2)
        //def uidMD5 = CryptoUtil.calculateMD5(clienteCodigoV2 + multisectionRecord['100000'].fechaEmision)
        def uidMD5 = TxtDigester.calcUidMD5(multisectionRecord)
        def record = [] // new List
        // --- Doc1 --- //
        record << "\"A1000\""
        // -- Video -- //
        record << "\"${uidMD5}\"" // UID('uid')
        record << { // NAME('name')
            String clienteNombre = multisectionRecord['100200'].clienteNombre.replaceAll('"', '')
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
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromCurrentMonth(multisectionRecord['100000'].fechaEmision)}\"" // DIAS_ACTUAL('diasActual')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromLastMonth(multisectionRecord['100000'].fechaEmision)}\"" // DIAS_ANTERIOR('diasAnterior')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd(multisectionRecord['100000'].periodoInicio)}\"" // DIA_INICIO('diaInicio')
        if (multisectionRecord['100000'].clienteCiclo == '10') {
            record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromCurrentMonth(multisectionRecord['100000'].fechaEmision)}\"" // DIA_FINALIZA('diaFinaliza')
        } else {
            record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd(multisectionRecord['100000'].periodoFin)}\"" // DIA_FINALIZA('diaFinaliza')
        }
        record << "\"${multisectionRecord['100000'].clienteCiclo}\"" // CICLO('ciclo')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM(multisectionRecord['100000'].fechaEmision)}\"" // MES_FACTURA('mesFactura')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_fromLastMonth(multisectionRecord['100000'].fechaEmision)}\"" // MES_ANTERIOR('mesAnterior')
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM(multisectionRecord['100000'].fechaEmision)}\"" // MES_ACTUAL('mesActual')
        record << "\"${multisectionRecord['100200'].clienteTelefono}\"" // TELEFONO('telefono')
        record << '"0.00"' // RENTA_MENSUAL('rentaMensual')
        record << '"0.00"' // PAQUETES_ADICIONALES('paquetesAdicionales')
        record << "\"${multisectionRecord['100300'].totalFinanciamiento}\"" // FINANCIAMIENTOS('financiamientos')
        record << "\"${multisectionRecord['100300'].saldoMora}\"" // INTERES_MORA('interesMora')
        record << '"0.00"' // OTROS_CARGOS('otrosCargos')
        record << "\"${multisectionRecord['100300'].totalAjustes}\"" // AJUSTES('ajustes')
        record << "\"${FormatUtil.currencyToNumberFormatter(multisectionRecord['100300'].totalFinal)}\"" // TOTAL_PAGO('totalPago')
        record << "\"${multisectionRecord['100000'].fechaLimitePago}\"" // FECHA_PAGO('fechaPago')
        record << "\"BASE_URL/personaWeb/user/authhandoff?cde=${clienteCodigoMD5}\"" // FACTURA_URL('facturaURL')
        record << '"https://gt.mipagoclaro.com/?utm_source=FACTURA&utm_medium=VIDEO&utm_campaign=DOCONE#/"' // PAGO_URL('pagoURL')
        if (multisectionRecord['200200'].detalleServicios) {
            record << "\"${multisectionRecord['200200'].detalleServicios[0].servicioNombre}\"" // PLAN('plan')
        }
        else {
            record << '" "' // PLAN('plan')
        }
        // -- Mail -- //
        record << "\"${FormatUtil.currencyToNumberFormatter(multisectionRecord['100300'].totalFinal)}\"" // BILLMONTH
        record << '"Q"' // MONEDA
        record << "\"${multisectionRecord['100000'].documentoNumero}\"" //  NUMFAC
        record << "\"${multisectionRecord['100000'].fechaLimitePago}\"" // ISSUEDDATE
        record << "\"${multisectionRecord['100200'].clienteTelefono}\"" // PHONE
        record << "\"${FormatUtil.dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_yyyy(multisectionRecord['100000'].fechaEmision)}\"" // MESFAC
        record << "\"${multisectionRecord['100000'].periodoInicio}\"" // PERIFACINI
        record << "\"${multisectionRecord['100000'].periodoFin}\"" // PERFACFIN
        record << "\"${multisectionRecord['100200'].clienteNombre.replaceAll(',', ' ').replaceAll('  ', ' ').replaceAll('"', '')}\"" // FULLNAME
        record << "\"${multisectionRecord['100200'].clienteEmail.toLowerCase()}\"" // EMAIL
        record << "\"BASE_URL/personaWeb/user/authhandoff?cde=${clienteCodigoMD5}\"" // SMART_URL
        record << "\"${clienteCodigoV2}\""// CLIENTE_CODIGO("CLIENTE_CODIGO")
        record << "\"${clienteCodigoMD5}\""// CLIENTE_CODIGO_MD5("CLIENTE_CODIGO_MD5")
        record << "\"${multisectionRecord['100200'].clienteNumeroTributario}\""// CLIENTE_NIT("CLIENTE_NIT")
        record << "\"VIDEO_URL/claro/embed-iframe.php?uid=${uidMD5}\"" // VIDEO_URL("VIDEO_URL")
        record << '"MOVIL"' // TELEFONIA("TELEFONIA"),
        record << "\"Factura_${multisectionRecord['100000'].fechaEmision.replaceAll('/', '')}_${multisectionRecord['100200'].clienteTelefono}.pdf\"" // ATTACHMENT("ATTACHMENT")
        record << "\"ORG_ID\"" //ORG_ID("ORG_ID")
        record << "\"${multisectionRecord['100400'].imprimirPeriodo}\"" // IMPRIMIR_PERIODO

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
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyyMMdd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_yyyyMM(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyyMM
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyyMM'))
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMMyyyy) {
        // dd/MM/yyyy -> yyyy-MM-dd
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', FormatUtil.locale))
        localDate.format(DateTimeFormatter.ofPattern('yyyy-MM-dd'))
    }

    static String dateFormatterSeparatedBySlash_dd_MMM_yyyy_TO_SeparatedByDash_yyyy_MM_dd(String date_ddMMMyyyy) {
        // dd/MMM/yyyy -> yyyy-MM-dd
        LocalDate localDate = LocalDate.parse(date_ddMMMyyyy, DateTimeFormatter.ofPattern('dd/MMM/yyyy', FormatUtil.locale))
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

    static String dateFormatter_ddMMM_TO_SeparatedBySlash_dd_MM_yyyy(String date_ddMMM, String refDate_ddMMyyyy) {
        // ddMMM -> dd/MM/yyyy

        YearMonth refYearMonth = YearMonth.parse(refDate_ddMMyyyy, DateTimeFormatter.ofPattern('MMMM/yyyy', locale))

        String date_ddMMyyyy = "${date_ddMMM.toLowerCase()}${refYearMonth.year}"
        LocalDate localDate = LocalDate.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('ddMMMyyyy', locale))
        localDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
    }

    static def dateFormatter_ddMMM_TO_SeparatedBySlash_dd_MM_yyyy(String iniDate_ddMMM, String endDate_ddMMM, String refDate_ddMMyyyy) {
        // ddMMM -> dd/MM/yyyy
        // Method return a Tuple2 instance.

        YearMonth refYearMonth = YearMonth.parse(refDate_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))

        YearMonth iniYearMonth = YearMonth.parse("${iniDate_ddMMM.toLowerCase()}${refYearMonth.year}", DateTimeFormatter.ofPattern('ddMMMyyyy', locale))
        YearMonth endYearMonth = YearMonth.parse("${endDate_ddMMM.toLowerCase()}${refYearMonth.year}", DateTimeFormatter.ofPattern('ddMMMyyyy', locale))

        String iniDate_ddMMyyyy = "${iniDate_ddMMM.toLowerCase()}${refYearMonth.year}"
        String endDate_ddMMyyyy = {
            if (iniYearMonth.month < endYearMonth.month) {
                // NOV - DIC
                return "${endDate_ddMMM.toLowerCase()}${refYearMonth.year}"
            } else {
                // DIC - ENE
                return "${endDate_ddMMM.toLowerCase()}${refYearMonth.year + 1}"
            }
        }.call()

        LocalDate iniLocalDate = LocalDate.parse(iniDate_ddMMyyyy, DateTimeFormatter.ofPattern('ddMMMyyyy', locale))
        LocalDate endLocalDate = LocalDate.parse(endDate_ddMMyyyy, DateTimeFormatter.ofPattern('ddMMMyyyy', locale))
        if (iniLocalDate.getDayOfMonth() == endLocalDate.getDayOfMonth()) {
            iniLocalDate = iniLocalDate.plusDays(1)
        }

        String iniDate = iniLocalDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))
        String endDate = endLocalDate.format(DateTimeFormatter.ofPattern('dd/MM/yyyy'))

        return new Tuple2(iniDate, endDate)
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_yyyy(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        String monthName_yyyy = yearMonth.format(DateTimeFormatter.ofPattern('MMMM yyyy', locale))
        return "${monthName_yyyy[0..<1].toUpperCase()}${monthName_yyyy[1..-1]}"
    }

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_MMMM_fromLastMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
        yearMonth = yearMonth.minusMonths(1)
        String monthName = yearMonth.format(DateTimeFormatter.ofPattern('MMMM', locale))
        return "${monthName[0..<1].toUpperCase()}${monthName[1..-1]}"
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

    static String dateFormatterSeparatedBySlash_dd_MM_yyyy_TO_dd_fromLastMonth(String date_ddMMyyyy) {
        YearMonth yearMonth = YearMonth.parse(date_ddMMyyyy, DateTimeFormatter.ofPattern('dd/MM/yyyy', locale))
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

}



class JsonUtil {
    static void writeJsonToFile(Path filePath, String json) {
        //createDirectories(filePath)
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

//            // NOTE: Block only for development purposes
//            String multisectionRecordJsonPrettyPrint = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multisectionRecord))
//            //println multisectionRecordJsonPrettyPrint
//            if (multisectionRecordJsonPrettyPrint) {
//                String jsonFilename = "${multisectionRecord['100000'].documentoNumero}.json"
//                Path jsonFilePath = this.absDirPath.resolve(jsonFilename)
//                JsonUtil.writeJsonToFile(jsonFilePath, multisectionRecordJsonPrettyPrint)
//            }

            String xml = XmlUtil.buildXml(multisectionRecord)
            if (xml) {
                // println groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multilineRecord.B1000[0]))
                String xmlFilename = "${multisectionRecord['100000'].documentoNumero}.xml"
                Path xmlFilePath = this.absDirPath.resolve(xmlFilename)
                XmlUtil.writeXmlToFile(xmlFilePath, xml)
            }
            // --
            boolean withChildren = XmlUtil.withChildren(multisectionRecord)
            withChildren = true // TODO: It's forced, comment line
            if (withChildren) {
                xml = XmlUtil.buildChildrenDoc1(multisectionRecord)
                if (xml) {
                    String xmlFilename = "${multisectionRecord['100000'].documentoNumero}-children.doc1"
                    Path xmlFilePath = this.absDirPath.resolve(xmlFilename)
                    XmlUtil.writeXmlToFile(xmlFilePath, xml)
                }
                // --
                xml = XmlUtil.buildChildrenSmartbill(multisectionRecord)
                if (xml) {
                    String xmlFilename = "${multisectionRecord['100000'].documentoNumero}-children.smartbill"
                    Path xmlFilePath = this.absDirPath.resolve(xmlFilename)
                    XmlUtil.writeXmlToFile(xmlFilePath, xml)
                }
            }

            String csvRecord = CsvUtil.buildCsvRecord(multisectionRecord)
            if (csvRecord) {
                String csvFilename = "${multisectionRecord['100000'].documentoNumero}.csv"
                Path csvFilePath = this.absDirPath.resolve(csvFilename)
                CsvUtil.writeCsvToFile(csvFilePath, csvRecord)
            }

            //CsvUtil.buildMasterCsv(multisectionRecord)
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
                if (line.startsWith(TxtSpecs.SectionType._100000.id)) {
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
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_PR_CM_GUATEMALA_C27_MAYO.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_PR_CM_GUATEMALA_C02_MAYO.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_FE_CM_GUATEMALA_C27_MAYO.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_FE_CM_GUATEMALA_C02_MAYO.txt'
        //filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_PR_CM_GUATEMALA_C14_JUNIO_Prepago.txt'
        filePathname = 'C:\\Users\\Omar Ochoa\\PROJECTS\\claro-guatemala-files\\TELEFONIA_MOVIL\\CLAROGT_FE_CM_GUATEMALA_C02_JULIO.txt'
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

        statusFilePath.write('PROCESSED\n')
        statusFilePath.append("${filesSize}")
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
        String childrenDoc1 = null
        String childrenSmartbill = null
        boolean withChildren = XmlUtil.withChildren(multisectionRecord)
        withChildren = true // NOTE: Force SQL query!!!
        if (withChildren) {
            childrenDoc1 = XmlUtil.buildChildrenDoc1(multisectionRecord)
            childrenSmartbill = XmlUtil.buildChildrenSmartbill(multisectionRecord)
        }
        String csvRecord = CsvUtil.buildCsvRecord(multisectionRecord)
        if (mainXml) {
            //println groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(multilineRecord.B1000[0]))
            data.mainXml = mainXml
            data.childrenDoc1 = childrenDoc1
            data.childrenSmartbill = childrenSmartbill
            data.withChildren = withChildren
            // --
            data.fechaEmision = multisectionRecord['100000'].fechaEmision
            data.clienteNombre = multisectionRecord['100200'].clienteNombre
            data.clienteCodigo = multisectionRecord['100200'].clienteCodigo
            data.clienteTelefono = multisectionRecord['100200'].clienteTelefono
            data.clienteId = multisectionRecord['100200'].clienteId
            data.clienteCiclo = multisectionRecord['100000'].clienteCiclo
            data.periodoInicio = multisectionRecord['100000'].periodoInicio // dd/MM/yyyy
            data.periodoFin = multisectionRecord['100000'].periodoFin // dd/MM/yyyy
            // --
            data.orgId = multisectionRecord['100200'].clienteCodigo.replaceAll('\\.', '')
            data.costCenter = multisectionRecord['100200'].clienteNumeroTributario
            data.clienteCategoria = multisectionRecord['100000'].clienteCategoria
            // --
            data.csvRecord = csvRecord
            // --
            data.phones = {
                List phones = []
                multisectionRecord['300000'].detalleConsumos.each { TxtDigester.SectionRecord detalleConsumo ->
                    phones << detalleConsumo.telefono.replace('-', '')
                }
                return phones
            }.call()
        } else {
            data.mainXml = ''
            data.childrenDoc1 = ''
            data.childrenSmartbill = ''
            data.withChildren = false
            // --
            data.detalleTrafico = ''
            data.fechaEmision = ''
            data.clienteNombre = ''
            data.clienteId = ''
            data.clienteCodigo = ''
            data.clienteTelefono = ''
            data.clienteCiclo = ''
            data.periodoInicio = ''
            data.periodoFin = ''
            // --
            data.orgId = ''
            data.costCenter = ''
            data.clienteCategoria =
            // --
            data.csvRecord = ''
            // --
            data.phones = []
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
// version 47
