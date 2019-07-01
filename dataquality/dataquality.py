import os 
import smtplib
from email.mime.text import MIMEText


def parse_filename(infile):
    """Parse an input throughput file name

    Parameters
    ----------
    infile: str
        Path to file to upload to the database

    Returns
    -------
    tab_name: str
        Name of input table

    obsdate: str
        Observing date

    """
    l = os.path.basename(infile).split('.')[0]
    l = l.split('_')
    if len(l) != 2:
        raise ValueError('{} is not named correctly'.format(infile))
    return l

def send_email(error_msg, subject='', server='mail.saao.ac.za', username='',
               password='', to='', bcc='', sender = 'sa@salt.ac.za'):
    """If an error or exception occured, send an email to report on the
       fault with the running of the program

 
    Parameters
    ----------
    error_msg: str
       Error message to be sent
 
    """
    
    smtp = smtplib.SMTP()
    smtp.connect(server)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()

    smtp.login(username,password)

    #set up the message
    msg = MIMEText(error_msg)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to
    msg['bcc'] = bcc

    #set up recipiants
    recip = [x for x in (to+','+bcc).split(',')]
    
    #send message
    smtp.sendmail(sender,recip,msg.as_string())
