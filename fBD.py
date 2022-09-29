from sqlalchemy import Integer, Column, String, false, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import func
from datetime import datetime
import pyodbc
# Funciones para base de datos
# iterar en bd- clase ModelBase para tabla Retenciones
# funcion add genera ingreso de data 1 a bd 2


engine = create_engine("sqlite:///BD/user2022.db", echo=False)
Base = declarative_base()
session = sessionmaker(bind=engine)
session = session()

def conex():
    server = 'localhost'
    user = 'sa'
    pas = '123456'
    db = 'SAWDB'
    try:
        conex = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server +
                               ';DATABASE='+db+';UID='+user+';PWD='+pas)
    except:
        print("ERROR CONEXION")
        pass
    return conex

def consulta1(n):
    # n="J293758203"
    # año ="2022"
    # mes="02"
    # dia="08"
    # f=año+"-"+mes+"-"+dia+" 00:00:00"
    # ver1=session.query(Prov).filter(Prov.CodigoProveedor==n , Prov.Fecha==f).first()
    r = []
    c = 0
    ver1 = (session.query(Prov).filter(Prov.NumeroDelDocumento == n ,Prov.ConsecutivoCompania==5 ).all())
    while ver1:
        r.insert(c, ver1)
        c = c+1
        return r

def consulta2(d, n):
    ver1 = []
    r = []
    c = 2
    ver1 = session.query(Prov_2).filter(
        Prov_2.rif == n, Prov_2.ConsecutivoCompania == d).one()
    while ver1:
        r.append(ver1)
        c = c+1
        return r

def consulta3(n):
    ver1 = []
    r = []
    ver1.append(session.query(Empresas).filter(
        Empresas.ConsecutivoCompania == n).one())
    while ver1:
        r = ver1
        return r
    """ver1.append(session.query(Empresas).filter(Empresas.ConsecutivoCompania==n).one())
    return ver1"""

def consulta4(d):
    ver1 = []
    r = []
    ver1=(session.query(Ret_Iva).filter(
        Ret_Iva.NumeroDeFactura==d
    ).all())
    return ver1

def consulta5():
    ver1= session.query(Prov).limit(10).all()
    return ver1
    
def iter_bd():
    cursor.execute("SELECT *FROM [SAWDB].[dbo].[IGV_Compra_RetencionesPorConcepto_ISLR]  where FechaDeInicioDeVigencia = (SELECT TOP 1 FechaDeInicioDeVigencia  FROM IGV_Compra_RetencionesPorConcepto_ISLR WHERE IGV_Compra_RetencionesPorConcepto_ISLR.FechaDeInicioDeVigencia < IGV_Compra_RetencionesPorConcepto_ISLR.Fecha ORDER BY IGV_Compra_RetencionesPorConcepto_ISLR.FechaDeInicioDeVigencia DESC)")
    consulta = cursor.fetchall()
    c = 0
    r = []
    while consulta:
        r.insert(c, consulta)
        c = c+1
        return r

def iter_bd2():
    cursor.execute("SELECT * from Proveedor")
    con2 = cursor.fetchall()
    c = 0
    r = []
    while con2:
        r.insert(c, con2)
        c = c+1
        return r

def iter_bd3():
    cursor.execute("SELECT * FROM COMPANIA")
    con2 = cursor.fetchall()
    c = 0
    r = []
    while con2:
        r.insert(c, con2)
        c = c+1
        return r

def iter_bd4():
    #cursor.execute("SET DATEFORMAT dmy  SELECT  cxP.ConsecutivoCompania, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ((cxP.MontoExento  + cxP.MontoGravado  + cxP.MontoIva )  * 1) ELSE ((cxP.MontoExento  + cxP.MontoGravado  + cxP.MontoIva )  * cxP.CambioABolivares) END ) As TotalCXPComprobanteRetIva, proveedor.Direccion, proveedor.Telefonos, proveedor.NumeroRIF, proveedor.NumeroNIT, proveedor.NombreProveedor, proveedor.CodigoProveedor, cxp.FechaAplicacionRetIVA, cxp.MesDeAplicacion, cxp.AnoDeAplicacion,  (CASE WHEN CAST(cxP.NumeroComprobanteRetencion AS int) <> 0 THEN cxP.NumeroComprobanteRetencion ELSE 'Sin N° Asignado' END )  AS NumeroComprobanteRetencion, cxP.PorcentajeRetencionAplicado, cxP.Fecha AS FechaDelDocOrigen,  (CASE WHEN cxP.TipoDeCxP = '0' THEN  (CASE WHEN cxP.UsaPrefijoSerie='S' THEN 'Serie ' + cxP.Numero ELSE cxP.Numero END )  ELSE '' END )  AS NumeroDeFactura, cxP.NumeroControl,  (CASE WHEN cxP.TipoDeCxP = '4' THEN cxP.Numero ELSE '' END )  AS NumeroDeNotaDebito,  (CASE WHEN cxP.TipoDeCxP = '3' THEN cxP.Numero ELSE '' END )  AS NumeroDeNotaCredito, (CASE  WHEN cxP.TipoDeTransaccion = '0' THEN '01 Registro'  WHEN cxP.TipoDeTransaccion = '1' THEN '02 Complemento'  WHEN cxP.TipoDeTransaccion = '2' THEN '03 Anulación'  WHEN cxP.TipoDeTransaccion = '3' THEN '04 Ajuste' END ) AS TipoDeTransaccion, cxP.NumeroDeFacturaAfectada, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoExento  * 1) ELSE (cxP.MontoExento  * cxP.CambioABolivares) END ) AS MontoExento, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravado  * 1) ELSE (cxP.MontoGravado  * cxP.CambioABolivares) END ) AS MontoGravado, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ( CASE WHEN cxP.MontoGravableAlicuotaGeneral  <> 0 THEN cxP.MontoGravableAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial2 <> 0 THEN cxP.MontoGravableAlicuotaEspecial2 ELSE cxP.MontoGravableAlicuotaEspecial1 END   END   * 1) ELSE ( CASE WHEN cxP.MontoGravableAlicuotaGeneral  <> 0 THEN cxP.MontoGravableAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial2 <> 0 THEN cxP.MontoGravableAlicuotaEspecial2 ELSE cxP.MontoGravableAlicuotaEspecial1 END   END   * cxP.CambioABolivares) END ) AS MontoGravableAlicuotaGeneral, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravableAlicuota2 * 1) ELSE (cxP.MontoGravableAlicuota2 * cxP.CambioABolivares) END ) AS MontoGravableAlicuota2, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravableAlicuota3 * 1) ELSE (cxP.MontoGravableAlicuota3 * cxP.CambioABolivares) END ) AS MontoGravableAlicuota3,  (CASE WHEN cxp.AplicaIvaAlicuotaEspecial  = 'N' THEN (SELECT TOP 1 alicuotaIVA.MontoAlicuotaGeneral FROM alicuotaIVA WHERE  alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial1  > 0 THEN cxP.PorcentajeIvaAlicuotaEspecial1  ELSE cxP.PorcentajeIvaAlicuotaEspecial2  END   END ) As AlicuotaG,(SELECT TOP 1 alicuotaIVA.MontoAlicuota2 FROM alicuotaIVA WHERE alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) AS Alicuota2, (SELECT TOP 1 alicuotaIVA.MontoAlicuota3 FROM alicuotaIVA WHERE alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) AS Alicuota3, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIva * 1) ELSE (cxP.MontoIva * cxP.CambioABolivares) END ) AS MontoIva, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ( CASE WHEN cxP.MontoIVAAlicuotaGeneral  <> 0 THEN cxP.MontoIVAAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoIVAAlicuotaEspecial2 <> 0 THEN cxP.MontoIVAAlicuotaEspecial2 ELSE cxP.MontoIVAAlicuotaEspecial1 END   END   * 1) ELSE ( CASE WHEN cxP.MontoIVAAlicuotaGeneral  <> 0 THEN cxP.MontoIVAAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoIVAAlicuotaEspecial2 <> 0 THEN cxP.MontoIVAAlicuotaEspecial2 ELSE cxP.MontoIVAAlicuotaEspecial1 END   END   * cxP.CambioABolivares) END ) AS MontoIVAAlicuotaGeneral, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIVAAlicuota2 * 1) ELSE (cxP.MontoIVAAlicuota2 * cxP.CambioABolivares) END ) AS MontoIVAAlicuota2, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIVAAlicuota3 * 1) ELSE (cxP.MontoIVAAlicuota3 * cxP.CambioABolivares) END ) AS MontoIVAAlicuota3, cxP.MontoRetenido,  YEAR  ( cxP.FechaAplicacionRetIva )  AS AnoAplicRetIVA, RIGHT('0' + CAST( MONTH  ( cxP.FechaAplicacionRetIva )  AS VARCHAR  ) , 2) AS MesAplicRetIVA  FROM proveedor INNER JOIN cxP ON ( proveedor.CodigoProveedor = cxP.CodigoProveedor) AND (proveedor.ConsecutivoCompania = cxP.ConsecutivoCompania) ")
    cursor.execute("SET DATEFORMAT dmy  SELECT cxP.ConsecutivoCompania, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ((cxP.MontoExento  + cxP.MontoGravado  + cxP.MontoIva )  * 1) ELSE ((cxP.MontoExento  + cxP.MontoGravado  + cxP.MontoIva )  * cxP.CambioABolivares) END ) As TotalCXPComprobanteRetIva, proveedor.Direccion, proveedor.Telefonos, proveedor.NumeroRIF, proveedor.NumeroNIT, proveedor.NombreProveedor, proveedor.CodigoProveedor, cxp.FechaAplicacionRetIVA, cxp.MesDeAplicacion, cxp.AnoDeAplicacion,  (CASE WHEN CAST(cxP.NumeroComprobanteRetencion AS int) <> 0 THEN cxP.NumeroComprobanteRetencion ELSE 'Sin N° Asignado' END )  AS NumeroComprobanteRetencion, cxP.PorcentajeRetencionAplicado, cxP.Fecha AS FechaDelDocOrigen,  (CASE WHEN cxP.TipoDeCxP = '0' THEN  (CASE WHEN cxP.UsaPrefijoSerie='S' THEN 'Serie ' + cxP.Numero ELSE cxP.Numero END )  ELSE '' END )  AS NumeroDeFactura, cxP.NumeroControl,  (CASE WHEN cxP.TipoDeCxP = '4' THEN cxP.Numero ELSE '' END )  AS NumeroDeNotaDebito,  (CASE WHEN cxP.TipoDeCxP = '3' THEN cxP.Numero ELSE '' END )  AS NumeroDeNotaCredito, (CASE  WHEN cxP.TipoDeTransaccion = '0' THEN '01 Registro'  WHEN cxP.TipoDeTransaccion = '1' THEN '02 Complemento'  WHEN cxP.TipoDeTransaccion = '2' THEN '03 Anulación'  WHEN cxP.TipoDeTransaccion = '3' THEN '04 Ajuste' END ) AS TipoDeTransaccion, cxP.NumeroDeFacturaAfectada, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoExento  * 1) ELSE (cxP.MontoExento  * cxP.CambioABolivares) END ) AS MontoExento, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravado  * 1) ELSE (cxP.MontoGravado  * cxP.CambioABolivares) END ) AS MontoGravado, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ( CASE WHEN cxP.MontoGravableAlicuotaGeneral  <> 0 THEN cxP.MontoGravableAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial2 <> 0 THEN cxP.MontoGravableAlicuotaEspecial2 ELSE cxP.MontoGravableAlicuotaEspecial1 END   END   * 1) ELSE ( CASE WHEN cxP.MontoGravableAlicuotaGeneral  <> 0 THEN cxP.MontoGravableAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial2 <> 0 THEN cxP.MontoGravableAlicuotaEspecial2 ELSE cxP.MontoGravableAlicuotaEspecial1 END   END   * cxP.CambioABolivares) END ) AS MontoGravableAlicuotaGeneral, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravableAlicuota2 * 1) ELSE (cxP.MontoGravableAlicuota2 * cxP.CambioABolivares) END ) AS MontoGravableAlicuota2, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoGravableAlicuota3 * 1) ELSE (cxP.MontoGravableAlicuota3 * cxP.CambioABolivares) END ) AS MontoGravableAlicuota3,  (CASE WHEN cxp.AplicaIvaAlicuotaEspecial  = 'N' THEN (SELECT TOP 1 alicuotaIVA.MontoAlicuotaGeneral FROM alicuotaIVA WHERE  alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) ELSE  CASE WHEN cxP.MontoGravableAlicuotaEspecial1  > 0 THEN cxP.PorcentajeIvaAlicuotaEspecial1  ELSE cxP.PorcentajeIvaAlicuotaEspecial2  END   END ) As AlicuotaG,(SELECT TOP 1 alicuotaIVA.MontoAlicuota2 FROM alicuotaIVA WHERE alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) AS Alicuota2, (SELECT TOP 1 alicuotaIVA.MontoAlicuota3 FROM alicuotaIVA WHERE alicuotaIVA.FechaDeInicioDeVigencia <= cxP.Fecha ORDER BY alicuotaIVA.FechaDeInicioDeVigencia DESC) AS Alicuota3, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIva * 1) ELSE (cxP.MontoIva * cxP.CambioABolivares) END ) AS MontoIva, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN ( CASE WHEN cxP.MontoIVAAlicuotaGeneral  <> 0 THEN cxP.MontoIVAAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoIVAAlicuotaEspecial2 <> 0 THEN cxP.MontoIVAAlicuotaEspecial2 ELSE cxP.MontoIVAAlicuotaEspecial1 END   END   * 1) ELSE ( CASE WHEN cxP.MontoIVAAlicuotaGeneral  <> 0 THEN cxP.MontoIVAAlicuotaGeneral  ELSE  CASE WHEN cxP.MontoIVAAlicuotaEspecial2 <> 0 THEN cxP.MontoIVAAlicuotaEspecial2 ELSE cxP.MontoIVAAlicuotaEspecial1 END   END   * cxP.CambioABolivares) END ) AS MontoIVAAlicuotaGeneral, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIVAAlicuota2 * 1) ELSE (cxP.MontoIVAAlicuota2 * cxP.CambioABolivares) END ) AS MontoIVAAlicuota2, ( CASE  WHEN cxP.Moneda = 'Bolívar' THEN (cxP.MontoIVAAlicuota3 * 1) ELSE (cxP.MontoIVAAlicuota3 * cxP.CambioABolivares) END ) AS MontoIVAAlicuota3, cxP.MontoRetenido,  YEAR  ( cxP.FechaAplicacionRetIva )  AS AnoAplicRetIVA, RIGHT('0' + CAST( MONTH  ( cxP.FechaAplicacionRetIva )  AS VARCHAR  ) , 2) AS MesAplicRetIVA  FROM proveedor INNER JOIN cxP ON ( proveedor.CodigoProveedor = cxP.CodigoProveedor) AND (proveedor.ConsecutivoCompania = cxP.ConsecutivoCompania) ")
    con2 = cursor.fetchall()
    c = 0
    r = []
    while con2:
        r.insert(c, con2)
        c = c+1
        return r

class Prov(Base):
    __tablename__ = "Retenciones"
    id = Column("id", Integer(), primary_key=True, unique=True)
    ConsecutivoCompania = Column(
        "ConsecutivoCompania", Integer(), nullable=False, unique=False)
    SecuencialDocPagado = Column(
        "SecuencialDocPagado", Integer(), nullable=False, unique=False)
    NumeroReferencia = Column("NumeroReferencia", String(10))
    NumeroComprobante = Column(
        "NumeroComprobante", Integer(), nullable=False, unique=False)
    NumeroDelDocumento = Column(
        "NumeroDelDocumento", String(10), nullable=False, unique=False)
    NumeroControl = Column("NumeroControl", String(10),
                           nullable=False, unique=False)
    MontoOriginal = Column("MontoOriginal", Float(),
                           nullable=False, unique=False)
    MontoBaseImponible = Column(
        "MontoBaseImponible", Float(), nullable=False, unique=False)
    MontoRetencion = Column("MontoRetencion", Float(),
                            nullable=False, unique=False)
    PorcentajeDeRetencion = Column(
        "PorcentajeDeRetencion", Float(), nullable=True, unique=False)
    Fecha = Column("Fecha", String())
    CodigoRetencion = Column("CodigoRetencion", String(),
                             nullable=False, unique=False)
    MesAplicacion = Column("MesAplicacion", Integer(),
                           nullable=False, unique=False)
    AnoAplicacion = Column("AnoAplicacion", Integer(),
                           nullable=False, unique=False)
    NombreProveedor = Column("NombreProveedor", String(),
                             nullable=False, unique=False)
    CodigoProveedor = Column("CodigoProveedor", String(),
                             nullable=False, unique=False)
    NumeroRIF = Column("NumeroRIF", String(), nullable=False, unique=False)
    TipoDePersona = Column("TipoDePersona", String(),
                           nullable=False, unique=False)
    FechaDeInicioDeVigencia = Column("FechaDeInicioDeVigencia", String())
    BaseImponible = Column("BaseImponible", Float(),
                           nullable=False, unique=False)
    Sustraendo = Column("Sustraendo", Float(), nullable=False, unique=False)
    TipoDePago = Column("TipoDePago", String(), nullable=False, unique=False)
    Tarifa = Column("Tarifa", Float(), nullable=False, unique=False)
    ParaPagosMayoresDe = Column(
        "ParaPagosMayoresDe", Float(), nullable=False, unique=False)

    def __init__(self, ConsecutivoCompania, SecuencialDocPagado, NumeroReferencia, NumeroComprobante, NumeroDelDocumento, NumeroControl, MontoOriginal, MontoBaseImponible, MontoRetencion, PorcentajeDeRetencion, Fecha, CodigoRetencion, MesAplicacion, AnoAplicacion, NombreProveedor, CodigoProveedor, NumeroRIF, TipoDePersona, FechaDeInicioDeVigencia, BaseImponible, Sustraendo, TipoDePago, Tarifa, ParaPagosMayoresDe):

        self.ConsecutivoCompania = ConsecutivoCompania
        self.SecuencialDocPagado = SecuencialDocPagado
        self.NumeroReferencia = NumeroReferencia
        self.NumeroComprobante = NumeroComprobante
        self.NumeroDelDocumento = NumeroDelDocumento
        self.NumeroControl = NumeroControl
        self.MontoOriginal = MontoOriginal
        self.MontoBaseImponible = MontoBaseImponible
        self.MontoRetencion = MontoRetencion
        self.PorcentajeDeRetencion = PorcentajeDeRetencion
        self.Fecha = Fecha
        self.CodigoRetencion = CodigoRetencion
        self.MesAplicacion = MesAplicacion
        self.AnoAplicacion = AnoAplicacion
        self.NombreProveedor = NombreProveedor
        self.CodigoProveedor = CodigoProveedor
        self.NumeroRIF = NumeroRIF
        self.TipoDePersona = TipoDePersona
        self.FechaDeInicioDeVigencia = FechaDeInicioDeVigencia
        self.BaseImponible = BaseImponible
        self.Sustraendo = Sustraendo
        self.TipoDePago = TipoDePago
        self.Tarifa = Tarifa
        self.ParaPagosMayoresDe = ParaPagosMayoresDe

    def __str__(self):
        return f'{self.ConsecutivoCompania} { self.SecuencialDocPagado} { self.NumeroReferencia} { self.NumeroComprobante} { self.NumeroDelDocumento} { self.NumeroControl} { self.MontoOriginal} { self.MontoBaseImponible} { self.MontoRetencion} { self.PorcentajeDeRetencion} { self.Fecha} { self.CodigoRetencion} { self.MesAplicacion} { self.AnoAplicacion} { self.NombreProveedor} { self.CodigoProveedor} { self.NumeroRIF} { self.TipoDePersona} { self.FechaDeInicioDeVigencia} { self.BaseImponible} { self.Sustraendo} { self.TipoDePago} { self.Tarifa} {self.ParaPagosMayoresDe} \n'

    def add(r):
        # Funcion que retorna los valores de la BD principal y copia en la BD2
        # print ("Actualizada tabla Retenciones.")
        c = 0
        k = 0
        i = len(r[0])
        for m in range(len(r[0])):
            user = Prov(ConsecutivoCompania=r[c][k][0],
                        SecuencialDocPagado=r[c][k][1],
                        NumeroReferencia=r[c][k][2],
                        NumeroComprobante=r[c][k][3],
                        NumeroDelDocumento=r[c][k][4],
                        NumeroControl=r[c][k][5],
                        MontoOriginal=r[c][k][6],
                        MontoBaseImponible=r[c][k][7],
                        MontoRetencion=r[c][k][8],
                        PorcentajeDeRetencion=r[c][k][9],
                        Fecha=r[c][k][10],
                        CodigoRetencion=r[c][k][11],
                        MesAplicacion=r[c][k][12],
                        AnoAplicacion=r[c][k][13],
                        NombreProveedor=r[c][k][14],
                        CodigoProveedor=r[c][k][15],
                        NumeroRIF=r[c][k][16],
                        TipoDePersona=r[c][k][17],
                        FechaDeInicioDeVigencia=r[c][k][18],
                        BaseImponible=r[c][k][19],
                        Sustraendo=r[c][k][20],
                        TipoDePago=r[c][k][21],
                        Tarifa=r[c][k][22],
                        ParaPagosMayoresDe=r[c][k][23])
            k = k+1
            session.add(user)
        session.commit()
        # print ("Actualizada tabla Retenciones.")

class Prov_2(Base):
    __tablename__ = 'Proveedores'

    id = Column("id", Integer(), primary_key=True, unique=True)
    ConsecutivoCompania = Column("ConsecutivoCompania", Integer())
    rif = Column("rif", String())
    mail = Column("mail", String())
    nProv = Column("nProv", String())
    phone = Column("phone", String())
    p_reten = Column("p_reten", Float())
    status = Column("status", Integer())
    fech_envio = Column("fech_envio", DateTime(), default=datetime.now)
    direc = Column("direc", String())

    def __init__(self, ConsecutivoCompania, rif, mail, nProv, phone, p_reten, status, direc):
        self.ConsecutivoCompania = ConsecutivoCompania
        self.rif = rif
        self.nProv = nProv
        self.phone = phone
        self.mail = mail
        self.p_reten = p_reten
        self.status = status
        self.direc = direc

    def __str__(self):
        return f'{self.ConsecutivoCompania} {self.id} {self.rif} {self.mail} {self.nProv}  {self.phone} {self.p_reten} {self.status} {self.direc}'

    def add_tab(d):
        # print ("Actualizando tabla proveedores.")
        c = 0
        k = 0
        i = len(d[0])
        for m in range(len(d[0])):
            da = Prov_2(
            ConsecutivoCompania=d[0][k][0],
            rif=d[0][k][1],
            nProv=d[0][k][3],
            phone=d[0][k][9],
            mail=d[0][k][12],
            p_reten=d[0][k][15],
            status=0,
            direc=d[0][k][10]
            )

            try:
                session.add(da)
            except:
                pass
            k = k+1
        session.commit()
        # print ("Actualizada tabla proveedores.")

class Empresas(Base):
    __tablename__ = "Empresas"
    id = Column("id", Integer(), primary_key=True, unique=True)
    ConsecutivoCompania = Column("ConcecutivoCompania", Integer())
    codigo = Column("codigo", String())
    NombreCompan = Column("NombreCompania", String())
    rif_compania = Column("rif", String())
    direc = Column("Direccion", String())
    ciudad = Column("Estado", String())

    def __init__(self, ConsecutivoCompania, codigo, NombreCompan, rif_compania, direc, ciudad):
        self.ConsecutivoCompania = ConsecutivoCompania
        self.codigo = codigo
        self.NombreCompan = NombreCompan
        self.rif_compania = rif_compania
        self.direc = direc
        self.ciudad = ciudad

    def __str__(self):
        return f'{self.ConsecutivoCompania} {self.codigo} {self.NombreCompan} {self.rif_compania} {self.direc} {self.ciudad}'

    def add_emp(d):
        # print ("Actualizando tabla proveedores.")
        c = 0
        k = 0
        i = len(d[0])
        for m in range(len(d[0])):

            da = Empresas(
            ConsecutivoCompania=d[0][k][0],
            codigo=d[0][k][1],
            NombreCompan=d[0][k][2],
            rif_compania=d[0][k][3],
            direc=d[0][k][6],
            ciudad=d[0][k][7])
            try:
                session.add(da)
            except:
                pass
            k = k+1
        session.commit()
        # print ("Actualizada tabla proveedores.")

class Ret_Iva(Base):
    __tablename__ = 'Ret_Iva'
    id = Column("id", Integer(), primary_key=True, unique=True)
    ConsecutivoCompania = Column("ConcecutivoCompania", Integer())
    TotalCXPComprobanteRetIva = Column(("TotalCXPComprobanteRetIva"), Float())
    direc=Column("Direccion",String())
    CodigoProveedor=Column("CodigoProveedor", String())
    FechaAplicacionRetIVA = Column("FechaAplicacionRetIVA", String())
    MesAplicacion=Column("MesAplicacion",Integer())
    AnoAplicacion=Column("AnoAplicacion",Integer())
    NumeroComprobanteRetencion=Column("NumeroComprobanteRetencion",String())
    PorcentajeRetencionAplicado=Column("PorcentajeRetencionAplicado",Float())
    FechaDelDocOrigen = Column("fech_envio", String())
    NumeroDeFactura =Column("NumeroDeFactura",String())
    NumeroControl = Column("NumeroControl", String(10))
    NumeroDeNotaDebito = Column("NumeroDeNotaDebito", String(15))
    NumeroDeNotaCredito = Column("NumeroDeNotaCredito", String(15))
    TipoDeTransaccion =Column("TipoDeTransaccion", String(15))
    NumeroDeFacturaAfectada=Column("NumeroDeFacturaAfectada",String())
    MontoExento=Column("MontoExento",Float())
    MontoGravado=Column("MontoGravado",Float())
    MontoGravableAlicuotaGeneral =Column("MontoGravableAlicuotaGeneral",Float())
    MontoGravableAlicuota2=Column("MontoGravableAlicuota2",Float())
    MontoGravableAlicuota3=Column("MontoGravableAlicuota3",Float())
    AlicuotaG=Column("AlicuotaG",Float())
    Alicuota2=Column("Alicuota2",Float())
    Alicuota3=Column("Alicuota3",Float())
    MontoIva=Column("MontoIva",Float())
    MontoIVAAlicuotaGeneral=Column("MontoIVAAlicuotaGeneral",Float())
    MontoIVAAlicuota2=Column("MontoIVAAlicuota2",Float())
    MontoIVAAlicuota3=Column("MontoIVAAlicuota3",Float())
    MontoRetenido=Column("MontoRetenido",Float())
    AnoAplicacionRetIva=Column("AnoAplicacionRetIva",String())
    MesAplicacionRetIva=Column("MesAplicacionRetIva",String())
   
   
    def __init__(self,ConsecutivoCompania,TotalCXPComprobanteRetIva,direc,CodigoProveedor,FechaAplicacionRetIVA,MesAplicacion,AnoAplicacion,NumeroComprobanteRetencion,PorcentajeRetencionAplicado,FechaDelDocOrigen,NumeroDeFactura,NumeroControl,NumeroDeNotaDebito,NumeroDeNotaCredito,TipoDeTransaccion,NumeroDeFacturaAfectada,MontoExento,MontoGravado,MontoGravableAlicuotaGeneral,MontoGravableAlicuota2,MontoGravableAlicuota3,AlicuotaG,Alicuota2,Alicuota3,MontoIva,MontoIVAAlicuotaGeneral,MontoIVAAlicuota2,MontoIVAAlicuota3,MontoRetenido,AnoAplicacionRetIva,MesAplicacionRetIva):
        
        self.ConsecutivoCompania    = ConsecutivoCompania
        self.TotalCXPComprobanteRetIva  = TotalCXPComprobanteRetIva
        self.direc  = direc
        self.CodigoProveedor    = CodigoProveedor
        self.FechaAplicacionRetIVA  = FechaAplicacionRetIVA
        self.MesAplicacion  = MesAplicacion
        self.AnoAplicacion  = AnoAplicacion
        self.NumeroComprobanteRetencion = NumeroComprobanteRetencion
        self.PorcentajeRetencionAplicado    = PorcentajeRetencionAplicado
        self.FechaDelDocOrigen  = FechaDelDocOrigen
        self.NumeroDeFactura    = NumeroDeFactura
        self.NumeroControl  = NumeroControl
        self.NumeroDeNotaDebito    = NumeroDeNotaDebito
        self.NumeroDeNotaCredito    = NumeroDeNotaCredito
        self.TipoDeTransaccion  = TipoDeTransaccion
        self.NumeroDeFacturaAfectada    = NumeroDeFacturaAfectada
        self.MontoExento    = MontoExento
        self.MontoGravado   = MontoGravado
        self.MontoGravableAlicuotaGeneral   = MontoGravableAlicuotaGeneral
        self.MontoGravableAlicuota2 = MontoGravableAlicuota2
        self.MontoGravableAlicuota3 = MontoGravableAlicuota3
        self.AlicuotaG  = AlicuotaG
        self.Alicuota2  = Alicuota2
        self.Alicuota3  = Alicuota3
        self.MontoIva   = MontoIva
        self.MontoIVAAlicuotaGeneral    = MontoIVAAlicuotaGeneral
        self.MontoIVAAlicuota2  = MontoIVAAlicuota2
        self.MontoIVAAlicuota3  = MontoIVAAlicuota3
        self.MontoRetenido  = MontoRetenido
        self.AnoAplicacionRetIva    = AnoAplicacionRetIva
        self.MesAplicacionRetIva    = MesAplicacionRetIva

    def __str__(self):
        return f'{self.id} {self.ConsecutivoCompania} {self.TotalCXPComprobanteRetIva} {self.direc} {self.CodigoProveedor} {self.FechaAplicacionRetIVA} {self.MesAplicacion} {self.AnoAplicacion} {self.NumeroComprobanteRetencion} {self.PorcentajeRetencionAplicado} {self.FechaDelDocOrigen} {self.NumeroDeFactura} {self.NumeroControl} {self.NumeroDeNotaDebito} {self.NumeroDeNotaCredito} {self.TipoDeTransaccion} {self.NumeroDeFacturaAfectada} {self.MontoExento} {self.MontoGravado} {self.MontoGravableAlicuotaGeneral} {self.MontoGravableAlicuota2} {self.MontoGravableAlicuota3} {self.AlicuotaG} {self.Alicuota2} {self.Alicuota3} {self.MontoIva} {self.MontoIVAAlicuotaGeneral} {self.MontoIVAAlicuota2} {self.MontoIVAAlicuota3} {self.MontoRetenido} {self.AnoAplicacionRetIva} {self.MesAplicacionRetIva} '    

    def add_Ret_Iva(d):
        # print ("Actualizando tabla proveedores.")
        c = 0
        k = 0
        i = len(d[0])

        for m in range(len(d[0])):

            da = Ret_Iva(
            ConsecutivoCompania=d[0][k][0],
            TotalCXPComprobanteRetIva=d[0][k][1],
            direc=d[0][k][2],
            CodigoProveedor=d[0][k][7],
            FechaAplicacionRetIVA=d[0][k][8],
            MesAplicacion=d[0][k][9],
            AnoAplicacion=d[0][k][10],
            NumeroComprobanteRetencion=d[0][k][11],
            PorcentajeRetencionAplicado=d[0][k][12],
            FechaDelDocOrigen=d[0][k][13],
            NumeroDeFactura=d[0][k][14],
            NumeroControl=d[0][k][15],
            NumeroDeNotaDebito=d[0][k][16],
            NumeroDeNotaCredito=d[0][k][17],
            TipoDeTransaccion=d[0][k][18],
            NumeroDeFacturaAfectada=d[0][k][19],
            MontoExento=d[0][k][20],
            MontoGravado=d[0][k][21],
            MontoGravableAlicuotaGeneral=d[0][k][22],
            MontoGravableAlicuota2=d[0][k][23],
            MontoGravableAlicuota3=d[0][k][24],
            AlicuotaG=d[0][k][25],
            Alicuota2=d[0][k][26],
            Alicuota3=d[0][k][27],
            MontoIva=d[0][k][28],
            MontoIVAAlicuotaGeneral=d[0][k][29],
            MontoIVAAlicuota2=d[0][k][30],
            MontoIVAAlicuota3=d[0][k][31],
            MontoRetenido=d[0][k][32],
            AnoAplicacionRetIva=d[0][k][33],
            MesAplicacionRetIva=d[0][k][34]
            )
            try:
                session.add(da)
            except:
                pass
            k = k+1
        session.commit()
        
        
        # print ("Actualizada tabla proveedores.")

class Correo_env(Base):
    __tablename__ = 'Correo_env'
    id = Column("id", Integer(), primary_key=True, unique=False)
    Correo = Column("correo", String())
    RIF =Column("RIF", String())
    Status=Column("Status", Integer())
    fech_envio = Column("fech_envio", DateTime(), default=datetime.now)
    n_Document=Column("n_Document", String())

    def __init__(self, Correo, RIF, Status,n_Document):
        self.Correo = Correo
        self.RIF=RIF
        self.Status=Status
        self.n_Document=n_Document


    def __str__(self):
        return f'{self.Correo} {self.RIF} {self.Status} {self.n_Document}'

    def Update_Status(correo,rif,stat,document):
        # print ("Actualizando tabla proveedores.")

        da = Correo_env(Correo=correo,RIF=rif,Status=stat,n_Document=document)

        session.add(da)
        session.commit()

def funciones():
    #base metadata solo aplica cuando se instale el programa
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Prov.add(iter_bd())
    Prov_2.add_tab(iter_bd2())
    Empresas.add_emp(iter_bd3()) 
    Ret_Iva.add_Ret_Iva(iter_bd4())
    
def close_conex():
    
    session.commit()
    cursor.close()
    conex.close()

conex = conex()
cursor = conex.cursor()

close_conex()

