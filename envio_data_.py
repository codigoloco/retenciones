import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


 
# Iniciamos los parámetros del script
def envio(rif,e_prov,e_prov2,e_prov3,documt):
    remitente = 'documentos@caltuca.com.ve'
    destinatarios = [e_prov, e_prov2,e_prov3]
    asunto = 'Retenciones correspondientes a Factura'+documt
    cuerpo = 'Saludos cordiales \t el siguiente tiene como finalidad la entrega de las retenciones de la factura'+ documt+"\t este es un correo automatizado toda respuesta generada debe realizarla a el contacto de Caltuca"
    
    ruta_adjunto = "./Output/"+rif+".pdf"
    #ruta_adjunto = ruta_a+rif+".pdf"
    
    
    nombre_adjunto = "Retenciones\t"+rif+".pdf"

    # Creamos el objeto mensaje
    mensaje = MIMEMultipart()
    
    # Establecemos los atributos del mensaje
    mensaje['From'] = remitente
    mensaje['To'] = ", ".join(destinatarios)
    mensaje['Subject'] = asunto
    
    # Agregamos el cuerpo del mensaje como objeto MIME de tipo texto
    mensaje.attach(MIMEText(cuerpo, 'plain'))
    # Abrimos el archivo que vamos a adjuntar
    archivo_adjunto = open(ruta_adjunto, 'rb')
    # Creamos un objeto MIME base
    adjunto_MIME = MIMEBase('application', 'octet-stream')
    # Y le cargamos el archivo adjunto
    adjunto_MIME.set_payload((archivo_adjunto).read())
    
    # Codificamos el objeto en BASE64
    encoders.encode_base64(adjunto_MIME)
    # Agregamos una cabecera al objeto
    adjunto_MIME.add_header('Content-Disposition', "attachment; filename= %s" % nombre_adjunto)
    # Y finalmente lo agregamos al mensaje
    mensaje.attach(adjunto_MIME)
    # Creamos la conexión con el servidor
    sesion_smtp = smtplib.SMTP('mail.clinicaccct.com', 587)
    # Ciframos la conexión
    sesion_smtp.starttls()
    # Iniciamos sesión en el servidor
    sesion_smtp.login(remitente,'Caltuca181014')
    # Convertimos el objeto mensaje a texto
    texto = mensaje.as_string()
    # Enviamos el mensaje
    sesion_smtp.sendmail(remitente, destinatarios, texto)
    # Cerramos la conexión
    sesion_smtp.quit()
    
        