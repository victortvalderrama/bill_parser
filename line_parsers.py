from functools import partial
from collections import namedtuple
from models import LineError, SECTION_NAME_MAP

# Helpers

def undefined_line(line_index, parsed):
    # print(f"Undefined behaviour at l: {line_index} for line id: {parsed.section_index}")
    # print(f"Predicate is: {parsed.predicate}")
    pass

def generic_predicate(bill, line_index, parsed, field_name):
    value = parsed.predicate.strip() + f"#{line_index}"
    setattr(bill, field_name, value)


def split_predicate(line_index, parsed, bill, split_len):
    tokens = parsed.predicate.split()
    if len(tokens) != split_len:
        error_msg = f"Could not split line in {split_len}"

        section_name = getattr(SECTION_NAME_MAP, parsed.section_index, "UNKNOWN SECTION")

        error = LineError(
            line_section_index=parsed.section_index,
            section_name=section_name,
            predicate=parsed.predicate,
            line_index=line_index,
            error=error_msg
        )

        bill.errors.append(error)
        return None
    return tokens


# SECTION 010000

def parse_0100000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill._01_id = parsed.section_index
    bill.segmentacion = parsed.predicate

def parse_0100100(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    line = parsed.predicate[82:]
    if len(line) == 15:
        line += " " *(29 - 15)
    
    bill.person_id = line[-9:].strip()
    bill.fecha_emision = line[:11]
    bill.imprime_ciclo = line[11:12]
    bill.con_inanciamiento = line[12:13]
    bill.tipoFactura = line[13:14]
    bill.imprimeBPS = line[14:15]
    bill.refactura = 'N'

    

parse_0100200 = partial(generic_predicate, field_name="emisor_nombre")
parse_0100300 = partial(generic_predicate, field_name="emisor_dir_l1")
parse_0100400 = partial(generic_predicate, field_name="emisor_dir_l2")

def parse_0100500(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)
    
    if not tokens:
        return

    bill.emisor_dir_l3 = tokens[0]
    bill.emisor_NIT = tokens[2]

def parse_0100600(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 3)

    if not tokens:
        return

    bill.factura_serie = tokens[2]
    
    if bill.factura_serie == 'NUMERO':
        bill.factura_serie = ' '
    
    bill.numero_autorizacion = tokens[-1]
    if bill.numero_autorizacion == 'AUTORIZACION:':
        bill.numero_autorizacion = ' '
    

def parse_0100700(bill, line_index, parsed):
    tokens = split_predicate(line_index, parsed, bill, 4)
    
    if not tokens:
        return
    
    bill.documento_numero = tokens[1]

    if bill.documento_numero == 'PARA':
        bill.documento_numero = ' '
        bill.documento_numero_V2 = bill.documento_numero.replace(',', '')
        bill.consultas = " ".join(tokens[1:-1])
    
    else:
        bill.documento_numero_V2 = bill.documento_numero.replace(',', '')
        bill.consultas = " ".join(tokens[2:-1])


def parse_0100800(bill, line_index, parsed):
    tokens = parsed.predicate.split()
    
    if not tokens:
        return
    
    refTokens = tokens[1].split('//')
    # print(refTokens)
    bill.ciclo = refTokens[0]
    bill.correlativo = refTokens[1]
    if len(tokens) == 8:
        bill.rutaCourier = tokens[2]
    else:
        bill.rutaCourier = ' '

parse_0100900 = partial(generic_predicate, field_name="receptor_nombre")

def parse_0101000(bill, line_index, parsed):
    # undefined_line(line_index, parsed)
    bill.receptorDirL1 = parsed.predicate[0:47].strip()
    bill.receptorTelefono = parsed.predicate[47:57].strip()

    data_total_pagar = parsed.predicate[57:78].strip()
    if data_total_pagar.startswith('Q-'):
        bill.totalPagar = "-" + data_total_pagar.split()[1]
    else:
        bill.totalPagar = data_total_pagar.split()[1]

    if len(parsed.predicate) == 89:
        bill.fechaVencimiento = parsed.predicate[78:89].strip()
    elif len(parsed.predicate) >= 88:
        bill.fechaVencimiento = parsed.predicate[78:90].strip()
    else:
        bill.fechaVencimiento = '01/ENE/1970'

    if len(parsed.predicate) >= 113:
        data_ajuste = parsed.predicate[95:].strip()
        if data_ajuste.startswith('Q-'):
            bill.ajuste = "-" + data_ajuste.split()[1]
        else:
            bill.ajuste = data_ajuste.split()[1]
    else:
        bill.ajuste = '0.00'
        
parse_0101100 = partial(generic_predicate, field_name="receptor_dir_l2")

def parse_0101200(bill, line_index, parsed):
    line = parsed.predicate
    bill.receptorDirL3 = line[0:39].strip()
    bill.receptorNIT = line[44:65].strip()
    bill.servicio = line[72:].strip()
    
parse_0101300 = partial(generic_predicate, field_name="limite")

def parse_0101400(bill, line_index, parsed):
    undefined_line(line_index, parsed)

parse_0101500 = partial(generic_predicate, field_name="mensaje")

def parse_0101600(bill, line_index, parsed):
    line = parsed.predicate
    bill.mensaje += line[33:].strip()

parse_0101700 = partial(generic_predicate, field_name="eMails")


# SECTION 020000


def parse_0200000(bill,line_index,parsed):
    # undefined_line(line_index, parsed)
    bill._02_id = parsed.section_index
        
def parse_0200100(bill,line_index,parsed):
    # 0200100POR VENTA DE SERVICIO TELEFONICO AL:  10/FEB/2021
    # undefined_line(line_index, parsed)
    tokens = parsed.predicate.split()
    bill.periodoFacturado = tokens[-1]
        
parse_0200200 = partial(generic_predicate, field_name="concepto_cobro")
# def parse_0200200(bill,line_index,parsed):
#     # 0200200RESUMEN CONCEPTO DE COBRO
#     #bill.conceptosCobro = [] # new List
#     undefined_line(line_index, parsed)
        
def parse_0200300(bill,line_index,parsed): # SALDO ANTERIORm
    tokens = split_predicate(line_index,parsed,bill,5)
    # bill.periodo = tokens.pop()
    # bill.conceptoCobrobill = tokens.pop()
    # conceptoCobrobill.valor = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    # conceptoCobrobill.nombre = tokens.join(' ')
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200400(bill,line_index,parsed): # SU PAGO GRACIAS
    tokens = split_predicate(line_index,parsed,bill,5)
    # TxtDigester.bill conceptoCobrobill = new TxtDigester.bill()
    # conceptoCobrobill.valor = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    # conceptoCobrobill.nombre = 'PAGOS EFECTUADOS (-)'
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200500(bill,line_index,parsed): # SALDO INICIAL
    tokens = split_predicate(line_index, parsed, bill, 5)
    # TxtDigester.bill conceptoCobrobill = new TxtDigester.bill()
    # conceptoCobrobill.periodo = tokens.pop()
    # conceptoCobrobill.valor = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    # conceptoCobrobill.nombre = tokens.join(' ')
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200600(bill,line_index,parsed): # CARGOS DEL MES
    tokens = split_predicate(line_index, parsed, bill, 3)
    # TxtDigester.bill conceptoCobrobill = new TxtDigester.bill()
    # conceptoCobrobill.nombre = tokens.join(' ')
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200700(bill,line_index,parsed): # FINANCIAMIENTO
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # TxtDigester.bill conceptoCobrobill = new TxtDigester.bill()
    # conceptoCobrobill.valor = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    # conceptoCobrobill.nombre = tokens.join(' ')
    # bill.conceptosCobro << conceptoCobrobill
        
def parse_0200800(bill,line_index,parsed): # SALDO INICIAL + FINANCIAMIENTO
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # TxtDigester.bill conceptoCobrobill = new TxtDigester.bill()
    # conceptoCobrobill.valor = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    # conceptoCobrobill.nombre = tokens.join(' ')
    # bill.conceptosCobro << conceptoCobrobill
    
# SECTION 030000

def parse_0300000(bill,line_index,parsed): # FINANCIAMIENTOS
    #sectionRecord.financiamientos = []
    undefined_line(line_index, parsed)
def parse_0300010(bill,line_index,parsed):
    # def tokens = line[7..-1].tokenize()
    # if (tokens.size() > 1) { # != TOTAL
    #     TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
    #     cargoMesSectionRecord.valor = {
    #         def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #         def currency = tokens[-1]
    #         if (currency == 'Q-') {
    #             valor = "-${valor}"
    #         }
    #         return valor
    #     }.call()
    #     if (tokens[-1] == 'Q' || tokens[-1] == 'Q-') {
    #         tokens.pop() # Remoe last token
    #     }
    #     cargoMesSectionRecord.nombre = tokens.join(' ')
    #     sectionRecord.financiamientos << cargoMesSectionRecord
    # } else { # == TOTAL
    #     TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
    #     cargoMesSectionRecord.nombre = tokens.join(' ')
    #     sectionRecord.financiamientos << cargoMesSectionRecord
    # }
    undefined_line(line_index, parsed)
def parse_0300100(bill,line_index,parsed): # PRODUCTOS Y SERVICIOS
    # sectionRecord.productosServicios = []
    undefined_line(line_index, parsed)
def parse_0300110(bill,line_index,parsed):
    # def tokens = line[7..-1].tokenize()
    # def text = []
    # # TODO: if 'VER DETALLE' changes it won't work
    # if (line.endsWith('VER DETALLE')) {
    #     text << tokens.pop() # remove last element
    #     text << tokens.pop() # remove last element
    # }
    # TxtDigester.SectionRecord cargoMesSectionRecord = new TxtDigester.SectionRecord()
    # if (!text.isEmpty()) {
    #     cargoMesSectionRecord.periodo = text.reverse().join(' ')
    # }
    # if (tokens.join(' ') ==~ /.*Q\s+\d*,?\d+\.\d{2}$/) { # Sample: Q         2,612.15
    #     cargoMesSectionRecord.valor = {
    #         def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #         def currency = tokens.pop()
    #         if (currency == 'Q-') {
    #             valor = "-${valor}"
    #         }
    #         return valor
    #     }.call()
    #     cargoMesSectionRecord.nombre = tokens.join(' ')
    #     sectionRecord.productosServicios << cargoMesSectionRecord
    # } else {
    #     cargoMesSectionRecord.periodo = 'N'
    #     cargoMesSectionRecord.valor = '0'
    #     cargoMesSectionRecord.nombre = tokens.join(' ')
    #     sectionRecord.productosServicios << cargoMesSectionRecord
    # }
    undefined_line(line_index, parsed)
def parse_0300150(bill,line_index,parsed):
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.totalFactura = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}"
    #     }
    #     return valor
    # }.call()
    undefined_line(line_index, parsed)
def parse_0300200(bill,line_index,parsed):
    # sectionRecord.aviso = line[7..-1].trim()
    undefined_line(line_index, parsed)
def parse_0300300(bill,line_index,parsed):
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.totalPagar = {
    #     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
    #     def currency = tokens.pop()
    #     if (currency == 'Q-') {
    #         valor = "-${valor}".toString()
    #     }
    #     return valor
    # }.call()
    undefined_line(line_index, parsed)
def parse_0400100(bill,line_index,parsed):
    # if (sectionRecord.containsKey('notificaciones')) { # List already exists?
    #     sectionRecord.notificaciones << line[7..-1] # Add item to list
    # } else {
    #     sectionRecord.notificaciones = [line[7..-1]] # New List with initial element
    # }
    undefined_line(line_index, parsed)

#


def parse_0400200(bill, line_index, parsed):
    undefined_line(line_index, parsed)
#     # sectionRecord.id = id
#     # # --
#     # def tokens = line[7..-1].tokenize()
#     # sectionRecord.serieAdministrativa = tokens[-1]
#     # break
def parse_0400300(bill, line_index, parsed):
    undefined_line(line_index, parsed)
#     # def tokens = line[7..-1].tokenize()
#     # while (tokens[-1] ==~ /[A-Za-z]+/) {
#     #     tokens.pop()
#     # }
#     # sectionRecord.numeroAdministrativo = tokens.pop()
#     # break
def parse_0400400(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.numeroAutorizacion = tokens[-1]
    # if (sectionRecord.numeroAutorizacion == 'AUTORIZACION:') {
    #     sectionRecord.numeroAutorizacion = ' '
    # }
    # break
def parse_0400410(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # sectionRecord.facturaSerie = line[21..50].trim()
    # if (sectionRecord.facturaSerie.isEmpty()) {
    #     sectionRecord.facturaSerie = ' '
    # }
    # sectionRecord.clienteNombre = line[51..-1]
    # break
def parse_0400500(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.telefono = tokens.pop()
    # break
def parse_0400600(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.mesFacturacion = tokens.pop()
    # break
def parse_0400700(bill, line_index, parsed):
    undefined_line(line_index, parsed)
#     def tokens = line[7..-1].tokenize()
# # sectionRecord.totalMes = FormatUtil.currencyToNumberFormatter(tokens.pop())
# # 20230110 - Jasiel - Cambio para tomar valores negativos
# sectionRecord.totalMes = {
#     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
#     def currency = tokens.pop()
#     if (currency == 'Q-') {
#         valor = "-${valor}"
#     }
#     return valor
# }.call()
# break
def parse_0400800(bill, line_index, parsed):
    undefined_line(line_index, parsed)
#     def tokens = line[7..-1].tokenize()
#     #sectionRecord.totalPagar = FormatUtil.currencyToNumberFormatter(tokens.pop())
#     # 20230110 - Jasiel - Cambio para tomar valores negativos
# sectionRecord.totalPagar = {
#     def valor = FormatUtil.currencyToNumberFormatter(tokens.pop())
#     def currency = tokens.pop()
#     if (currency == 'Q-') {
#         valor = "-${valor}"
#     }
#     return valor
# }.call()
# break
def parse_0401000(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.vencimiento = tokens.pop()
    # break
def parse_0401098(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.certificadorNIT = tokens.pop().tokenize(':').pop()
    # tokens.remove(0)
    # sectionRecord.certificador = tokens.join(' ')
    # break
def parse_0401099(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.numeroAdministrativo = tokens.pop()
    # if (sectionRecord.numeroAdministrativo == 'ADMINISTRATIVO:') {
    #     sectionRecord.numeroAdministrativo = ' '
    # }
    # break
def parse_0401100(bill, line_index, parsed):
    undefined_line(line_index, parsed)
    # def tokens = line[7..-1].tokenize()
    # sectionRecord.serieAdministrativa = tokens.pop()
    # if (sectionRecord.serieAdministrativa == 'ADMINISTRATIVA:') {
    #     sectionRecord.serieAdministrativa = ' '
    # }
    # break