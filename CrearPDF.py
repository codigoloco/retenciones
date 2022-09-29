import jinja2
import pdfkit
from fBD import session, Prov_2, Prov, consulta1, consulta2, consulta3, consulta4,consulta5
from datetime import date, time, datetime
from PyPDF2 import PdfFileMerger, PdfFileReader
import os
from fpdf import FPDF
import base64
from envio_data_ import envio
from fBD import Correo_env 

#Unir PDF Genera la union de el pdf 1 y 2 en un solo documento
def del_Arc():
    li= os.listdir('./archivos/')
    for file in li:
        os.remove('./archivos/'+file)

def unirpdf(contador,document):
    
    li= os.listdir('./archivos/')
    merger = PdfFileMerger()
    if len(li)>1:
        for file in li:
            merger.append(PdfFileReader('./archivos/'+file))
            os.remove('./archivos/'+file)
        merger.write('./Output/'+ contador+'.pdf')
        #envio(contador, "e.blanco@clinicaccct.com", "alexander.mendez@5cti.com.ve","jose.sebrihant@5cti.com.ve", document)
    elif  len(li)<3:pass
    
    """        envio_(contador, document)
        
    def envio_(contador,document):
        envio(contador,"e.blanco@clinicaccct.com"," ",document)
        Correo_env.Update_Status(c[0].mail, r[0][0].CodigoProveedor, 1,document)"""
#Data-N maneja la vinculacion de datos para ser optenidas por el template
def data():
    a = {
        "op":c[0].ConsecutivoCompania,
        "ciud": f[0].ciudad,
        "nro_comp": r[0][0].NumeroComprobante,
        "fecha_a": d,

        "r_social": f[0].NombreCompan,
        "RIF": f[0].rif_compania,
        "direccion": f[0].direc,
        "telf": " ",

        "r_social_prov": c[0].nProv,
        "RIF_prov": c[0].rif,
        "dir_prov": c[0].direc,
        "telf_prov": c[0].phone,
        "detail":[r[0]],
        "fecha_origen":F_docOrigen,
        "res":res,
        "monto":conv_mil(monto),
    }
    return a

def data2():
    info = {
        "f_ini": d,
        "N_Compania": f[0].NombreCompan,
        "R_Compania": f[0].rif_compania,
        "Ciudad": f[0].ciudad,
        "mes": ret_iva[0].MesAplicacionRetIva,
        "año": ret_iva[0].AnoAplicacionRetIva,
        "N_comprobante": ret_iva[0].NumeroComprobanteRetencion,

        "r_Agente": f[0].rif_compania,
        "N_Agente": "",
        "D_Agente": f[0].direc,
        "T_Agente": "",

        "N_Benef": c[0].nProv,
        "r_Benef": c[0].rif,
        "D_Benef": ret_iva[0].direc,
        "T_Benef": c[0].phone,
        "op":c[0].ConsecutivoCompania,
        "detail":[
         1,
         F_docOrigen,
         ret_iva[0].NumeroDeFactura,
         ret_iva[0].NumeroControl,
         ret_iva[0].NumeroDeNotaDebito,
         ret_iva[0].NumeroDeNotaCredito,
         ret_iva[0].TipoDeTransaccion,
         ret_iva[0].NumeroDeFacturaAfectada,
         ret_iva[0].NumeroComprobanteRetencion,
         ret_iva[0].PorcentajeRetencionAplicado,
         conv_mil( ret_iva[0].TotalCXPComprobanteRetIva),
         conv_mil(ret_iva[0].MontoExento),
         conv_mil(ret_iva[0].MontoGravado),
         ret_iva[0].AlicuotaG,
         conv_mil(ret_iva[0].MontoIVAAlicuotaGeneral),
         conv_mil(ret_iva[0].MontoRetenido)]
       }
 
    return info
#Crear PDF tienen las rutinas para  generar el PDF de primera instancia individuales
#cada uno tiene configurado su template y rutas necesarias
def Crea_PDF(ruta_template,root_salida, info, rutacss=""):

    n_template = ruta_template.split('/')[-1]
    ruta_template = ruta_template.replace(n_template, '')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(ruta_template))
    template = env.get_template(n_template)
    html = template.render(info)
    options = {'page-size': 'Letter',
               'margin-top': '0.40in',
               'margin-bottom': '0.1in',
               'margin-right': '0.8in',
               'margin-left': '0.80in',
               'encoding': 'UTF-8'
               }
    # Ruta de programacion WKHTMLTOPDF esta se mantiene fija
    config = pdfkit.configuration(
        wkhtmltopdf="C:\Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe")
    
    # Ruta Salida del archivo PDF
    #ruta_salida = './archivos/'+"ISLR"+r[0][0].NumeroRIF+" "+c[0].nProv+'.pdf'
    ruta_salida = root_salida+"ISLR"+' '+r[0][0].NumeroRIF+" "+c[0].nProv+'.pdf'
    pdfkit.from_string(html, ruta_salida, css=rutacss,
                       options=options, configuration=config)

def Crea_PDF2(ruta_template,root_salida, info, rutacss=""):
    n_template = ruta_template.split('/')[-1]
    ruta_template = ruta_template.replace(n_template, '')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(ruta_template))
    template = env.get_template(n_template)
    html = template.render(info)
    options = {'page-size': 'Letter',
               'orientation': 'Landscape',
               'margin-top': '0.40in',
               'margin-bottom': '0.1in',
               'margin-right': '0.8in',
               'margin-left': '0.80in',
               'encoding': 'UTF-8'
               }
    # Ruta de programacion WKHTMLTOPDF esta se mantiene fija
    config = pdfkit.configuration(
        wkhtmltopdf="C:\Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe")

    # Ruta Salida del archivo PDF
    ruta_salida = root_salida+"IVA"+' '+r[0][0].NumeroRIF+" "+c[0].nProv+'.pdf'
    pdfkit.from_string(html, ruta_salida, css=rutacss,
                       options=options, configuration=config)

def conv_mil(a):
    a=round(a,2)
    miles_translator = str.maketrans(".,", ",.")
    numero = "{:,}".format(a).translate(miles_translator)
    return numero

if __name__ == "__main__":
    
    
    ruta_template = './Template/template.html'
    ruta_template2 = './Template/template3.html'
    for i in consulta5():
        del_Arc()  
        val=0    
        cons = i.NumeroDelDocumento
        #cons="001294"
        r = (consulta1(cons))
        c = consulta2(r[0][0].ConsecutivoCompania, r[0][0].CodigoProveedor)
        f = consulta3(r[0][0].ConsecutivoCompania)
        d = datetime.strptime(r[0][0].Fecha, '%Y-%m-%d %H:%M:%S')
        d = d.strftime(('%d/%m/%Y'))
        ret_iva = (consulta4(cons))
        """    print(r[0][0])
        print (c[0])
        print(f[0][0])
        print(ret_iva[0])"""
        
        F_docOrigen = datetime.strptime(ret_iva[0].FechaDelDocOrigen, '%Y-%m-%d %H:%M:%S')
        F_docOrigen = F_docOrigen.strftime(('%d/%m/%Y'))
        #for i in r[0]:
        contador=r[0][0].NumeroRIF+" "+c[0].nProv+" "+cons
        res=0
        print(r[0][0].MontoBaseImponible)
        print(r[0][0].MontoBaseImponible.replace())
        for i in r[0]:
            res=i.MontoBaseImponible+res
        monto=0
        for i in r[0]:
            monto= i.MontoRetencion+monto
        root_salida='./archivos/'
        if r[0][0].MontoRetencion !=0:
            Crea_PDF(ruta_template,root_salida, data())
            val=val+1
        else: pass
        
        if ret_iva[0].MontoRetenido!=0:
            Crea_PDF2(ruta_template2,root_salida, data2())
            val=val+1
        else:pass
        if val >0:
            unirpdf(contador,i.NumeroDelDocumento)
        #print(c[0].rif," ",cons)



