import requests
import json
import logging
import configparser
from datetime import datetime,timedelta
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To,Bcc,Cc, Content, Attachment,Personalization,FileContent,FileName,FileType,Disposition
import concurrent.futures as TP
import os
import sys
import configparser
from pyspark.sql import SparkSession
import psycopg2
import re,random

# Placeholder for your file paths (replace these with actual paths or environment variables)
server_path = '/path/to/your/config'
config_file = os.path.join(server_path, 'promotion_pipeline_v1.ini')
creds_file = '/path/to/your/master_config_file.ini'

# Initialize configuration parsers
config = configparser.ConfigParser()
creds = configparser.ConfigParser()

'''pass country as argument to this job to process onboards for country specific programs i.e. US, CANADA '''
country =  sys.argv[1]
'''pass environment as second argument to this job to process onboards for country specific programs i.e. OLTP, OLTPSANDBOX '''
env = sys.argv[2]

host = creds['key']['hostname']
port = creds['key']['port']
user = creds['key']['username']
password = creds['key']['password']
client_id = creds['key']['client_id']
client_secret = creds['key']['client_secret']

aad_promotion = config['key']['aad_promotion'].format(country,'%')
driver = config['key']['driver']

spark = SparkSession.builder.master("local").appName("promotion_pipeline_v1").getOrCreate()
spark.sparkContext.setLogLevel('FATAL')
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.debug("initializing promotion_pipeline spark job..")

url = 'jdbc:postgresql://{0}:{1}/{2}?currentSchema={3}'.format(host,port,env,'schema')
log_path = config['path']['log_path']

config_path= config['path']['config_path']

def job_logger(msg,lvl='info'):
    logging.basicConfig(filename=os.path.join(log_path,'{}_{}_{}.log'.format('promotion_pipeline',env,datetime.now().strftime('%Y%m%d'))),
                format='%(asctime)s - %(message)s',
                filemode='a')
    # create logger object
    logger =  logging.getLogger("py4j")
    # Setting threshold of logger
    logger.setLevel(logging.INFO)
    if lvl == 'info':
        logger.info(msg)
    elif lvl == 'debug':
        logger.info(msg)
    elif lvl == 'error':
        logger.info(msg)
    elif lvl == 'warning':
        logger.info(msg)
    else:
        logger.info(msg)


def get_access_token_aad():
    with open(r"/your/path/access_token_.json","r") as inputfile:
        return json.load(inputfile)

aad_token = get_access_token_aad()['access_token']

def populate_sendgrid_log(message,status,data):
    '''internal pipeline messaging method upon failures or Fatal Logging level'''
    sendgrid_key='' # add your Sendgrid key
    send_grid_cli = sendgrid.SendGridAPIClient(api_key=sendgrid_key)
    nickname = data['nickname']
    lastname = data['lastname']
    firstname = data['firstname']
    legalname = firstname + " " + lastname
    workcontact = data['workcontact']
    costname = data['costname']
    mail_license = data['ms_license']
    personalcontact = data['personalcontact']
    oslemployeeid = data['oslemployeeid']
    locationstate = data['locationstate']
    from_email = Email("noreply@yourknow.com","ERRORS")  # Change to your verified sender
    mail_body = None
    subject = "AAD Error code {} - {} {} {}".format(status, legalname,costname,locationstate)
    if "does not have any available licenses." in message:
        to_email = [To()]  # Change to your recipient
        mail_body = "{}: {} \n\n{}{}\n\n{}\n{}\n{}".format(message,mail_license,'This issue came up while attaching license to:',workcontact,legalname,personalcontact,oslemployeeid)
    else:
        to_email = [To()]  # Change to your recipient
        mail_body = message
    content = Content("text/plain", mail_body)
    to_email = [To(""),To()]  # Change to your recipient
    mail = Mail(from_email, to_email, subject, content)
    #Get a JSON-ready representation of the Mail object
    mail_json = mail.get()
    # Send an HTTP POST request to /mail/send
    response = send_grid_cli.client.mail.send.post(request_body=mail_json)
    pass

def notify_IT(status,employee,payload,workcontact):
    '''internal pipeline messaging method for notifying other departments'''
    response = send_grid_cli.client.mail.send.post(request_body=mail_json)
    pass

def get_profiles(stmt):
    '''fetch profiles from employee profile '''
    df = spark.read.format("jdbc") \
                .option("url", url) \
                .option("query", stmt) \
                .option("user", user) \
                .option("password", password)\
                .option("driver", driver) \
                .load()
    collect_ = df.distinct().collect()
    profile_,all_profiles = {},[]
    for i in collect_:
        empty_= i.asDict()
        profile_[empty_['oslemployeeid']] = empty_
    for k,v in profile_.items():
        all_profiles.append({k:v})
    return all_profiles

def execute_powershell(azure_field,value,user_email):
    command = 'powershell -File "/your/powershellscript/ExchangeOnline.ps1" -varAttr "Set-Mailbox -Identity {} -Custom{} \'{}\'"'.format(user_email,azure_field,value)
    result = os.system(command)
    if result == 0: 
        return "Success"
    else:
        return None

def aad_pending(token,data):
    url = "https://graph.microsoft.com/v1.0/users/{}".format(data['workcontact_prev'])
    job_logger("url : {}".format(url))
    payload = {"accountEnabled": True,\
                "displayName": data['fullname'],\
                "givenName": data['firstname'],\
                "surname": data['lastname'],\
                "userPrincipalName": data['workcontact'],\
                "mailNickname": data['nickname'].replace(" ",""),\
                "usageLocation":data['empcountry'],\
                "jobTitle": data['jobtitle'],\
                "companyName": data['companyname'],\
                "department": data['department'],\
                "employeeId": str(data['oslemployeeid']),\
                "employeeType": data['aad_employeetype'],\
                "employeeHireDate": data['hiredate'],\
                "officeLocation": data['homecostnbrcode'],\
                "state": data['empstateabv'],\
                "country": data['empcountry'],\
                "onPremisesExtensionAttributes": {"extensionAttribute1": data['language'],\
                                                    "extensionAttribute2": data['field_corporate'],\
                                                    "extensionAttribute3": data['peoplemanager'],\
                                                    "extensionAttribute4": data['division'],\
                                                    "extensionAttribute5": data['region'],\
                                                    "extensionAttribute6": data['district'],\
                                                    "extensionAttribute7": data['costname'],\
                                                    "extensionAttribute8":data['firstname'],\
                                                    "extensionAttribute9": data['lastname'],\
                                                    "extensionAttribute10": data['pein'],\
                                                    "extensionAttribute11": data['client_code'],\
                                                    "extensionAttribute12": data['program_code'],\
                                                    "extensionAttribute13": "{}".format(json.dumps(build_json(data)))}}
    payload = json.dumps(payload)
    job_logger("payload : {} ".format(payload))
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' +token}
    response = requests.request("PATCH", url, headers=headers, data=payload)
    status = response.status_code
    if status in range (200,299):
        job_logger('API status code on updating AAD account :{0} \n{1}'.format(status, response.text))
        update_employee_profile(data['oslemployeeid'],'MANAGER UPDATE',data['workcontact'])
        manager_update(token,data)
    else:
        aad_error = response.json()['error']['message']
        if 'Unable to update the specified properties for objects that have originated within an external service' in aad_error:
            payload = json.loads(payload)
            attr = {key: value for key, value in payload.items() if key == "onPremisesExtensionAttributes"}
            for subkey in attr.values():
                for k,value in subkey.items():
                    field = k.replace('extension','')
                    if 'Attribute13' in k:
                        value = str(value)
                        print(value)
                        value = value.replace('\"','\\"')
                    job_logger("Changing {} to {} Azure.".format(field, value))
                    powershell = execute_powershell(field,value,data['workcontact_prev'])
                    if not powershell:
                        notify_IT(response.status_code,data['fullname'],payload,data['workcontact'])
                        break
            payload = {"accountEnabled": True,\
                "displayName": data['fullname'],\
                "givenName": data['firstname'],\
                "surname": data['lastname'],\
                "userPrincipalName": data['workcontact'],\
                "mailNickname": data['nickname'].replace(" ",""),\
                "usageLocation":data['empcountry'],\
                "jobTitle": data['jobtitle'],\
                "companyName": data['companyname'],\
                "department": data['department'],\
                "employeeId": str(data['oslemployeeid']),\
                "employeeType": data['aad_employeetype'],\
                "employeeHireDate": data['hiredate'],\
                "officeLocation": data['homecostnbrcode'],\
                "state": data['empstateabv'],\
                "country": data['empcountry']}
            payload = json.dumps(payload)
            job_logger("payload : {} ".format(payload))
            headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' +token}
            response = requests.request("PATCH", url, headers=headers, data=payload)
            update_employee_profile(data['oslemployeeid'],'MANAGER UPDATE',data['workcontact'])
            manager_update(token,data)
        populate_sendgrid_log(aad_error,status,data)
        job_logger('API status code on updating  AAD account :{0} \n{1}'.format(status, response.text))

def build_json(row,super_user=None):
     # INTERNAL LOGIC FOR APPLICATION GROUPS
    job_logger('OSL EMP ID : ',row['oslemployeeid'])
    job_logger(json.dumps(backbone_group))

    return backbone_group

def manager_update(token,data):
    url = "https://graph.microsoft.com/v1.0/users/{0}/manager/$ref".format(data['workcontact'])
    payload = json.dumps({
        "@odata.id": "https://graph.microsoft.com/v1.0/users/{0}".format(data['manageremail'])
        })
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer '+ token
               }
    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code in range(200,209):
        job_logger('API status code on updating manager AAD ID :{0}'.format(response.status_code))
        update_employee_profile(data['oslemployeeid'],'ATTACH LICENSE',data['workcontact'])
        attach_license(token,data)
    else:
        aad_error = response.json()['error']['message']
        # populate_sendgrid_log(aad_error,status,data)
        job_logger('API status code on updating manager AAD ID :{0} \n{1}'.format(response.status_code, response.text))

def notify_email_change(employee,workcontact,emp_id):
    '''internal pipeline messaging method for notifying about email change'''
    response = send_grid_cli.client.mail.send.post(request_body=mail_json)
    pass

def license_cleaup(token,object_id):
    skuid_list = []
    url = "https://graph.microsoft.com/v1.0/users/{0}/licenseDetails".format(object_id)
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + token
               }
    response = requests.request("GET", url, headers=headers)
    if response.status_code in range(200,299):
        licenses = response.json().get('value', [])
        if licenses:
            for license in licenses:
                skuid = license.get('skuId',None)
                if skuid:
                    skuid_list.append(skuid)
            job_logger("License attached to {} are as following: {}".format(object_id,skuid_list))
            job_logger(skuid_list)
    if skuid_list:
        url = "https://graph.microsoft.com/v1.0/users/{0}/assignLicense".format(object_id)
        payload = json.dumps({"addLicenses": [],
                          "removeLicenses": ["{}".format(', '.join(["'{}'".format(x) for x in skuid_list]))]
                          })
        response = requests.request("POST", url, headers=headers, data=payload)
        job_logger(response.status_code)
        if response.status_code in range(200,299):
            job_logger("License {} was removed from profile {}.".format(skuid_list,object_id))
        return True
    else:
        job_logger("Failed to remove license: {}".format(response.text))
        return False

def attach_license(token,data):
    objectId = data['workcontact']
    skuid = data['skuid']
    license_cleaup(token,objectId)
    notify_email_change(data['fullname'],data['workcontact'],data['oslemployeeid'])
    url = "https://graph.microsoft.com/v1.0/users/{0}/assignLicense".format(objectId)
    job_logger('Endpoint attach license:{0} '.format(url))
    payload = json.dumps({"addLicenses": [{"skuId": skuid}],
                          "removeLicenses": []
                          })
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + token
               }
    response = requests.request("POST", url, headers=headers, data=payload)
    status = response.status_code
    if status >= 200 and status <=299:
        job_logger('API status code on attaching license to AAD ID :{0}'.format(status))
        update_employee_profile(data['oslemployeeid'],'ACTIVE PROFILE',data['workcontact'])
    else:
        aad_error = response.json()['error']['message']
        populate_sendgrid_log(aad_error,status,data)
        job_logger('API status code on attaching license to AAD ID :{0} \n{1}'.format(status, response.text))
    
def update_employee_profile(employeeid, status,workcontact,aadid=None):
    # INTERNAL LOGIC FOR DATA TABLE UPDATE
    
def get_email_list(url=url,username=user,password=password,driver=driver):
    # INTERNAL LOGIC FOR GETTING ALL EXISTING EMAILS
    print('Distinct Email counts: {}'.format(len(emails)))
    return emails

email_list = get_email_list()   

def gen_email(firstname,lastname,domain,nickname=None,email_list=email_list,retry=1):
    # INTERNAL LOGIC FOR GENERATING NEW EMAIL
    while retry <= 15:
         return email
    else:
        return None

def init_pipeline(data,token=aad_token):
    for key in data.keys():
        job_logger(data[key]['firstname'] + ' '+ data[key]['lastname'] + ' ' + str(data[key]['rehiretype']))
        if data[key]['workcontact'] == '' or data[key]['workcontact'] is None:
            data[key]['workcontact'] = gen_email(data[key]['firstname'],data[key]['lastname'],data[key]['Domain'],data[key]['nickname'])
        globals()[data[key]['status']](token,data[key])
    pass


def main():
    emp_list = get_profiles(aad_promotion)
    then_=datetime.now()
    for data in emp_list:
        init_pipeline(data)
    now_=datetime.now()
    job_logger('Took {} seconds to complete tasks.'.format((now_-then_).total_seconds()))
    os.system('tail -25 ' + os.path.join(log_path,'{}_{}_{}.log'.format('promotion_pipeline',env,datetime.now().strftime('%Y%m%d'))))

if __name__ == '__main__':
    main()