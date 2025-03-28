import requests
import argparse
import signal
import readchar
import time
import os
import pandas as pd
from requests_oauthlib import OAuth1
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import init, Fore, Back, Style
import pprint
from openpyxl import load_workbook
import multiprocessing
import re

init()

def write_log(lines_to_write):
    file_log = open('ExecutionLog.txt', 'w')
    file_log.writelines(lines_to_write)
    file_log.close()

def print_log(log_type, message, exit_on_error = False):
    if message and log_type:
        if log_type == 'info':
            mssg_to_print = Fore.BLUE + f"[!] {message}"
        elif log_type == 'success':
            mssg_to_print = Fore.GREEN + f"[+] {message}"
        elif log_type == 'error':
            mssg_to_print = Fore.RED + f"[x] {message}"
            if exit_on_error:
                write_log(mssg_to_print + '\n')
                exit(1)
        elif log_type == 'warning':
            mssg_to_print = Fore.YELLOW + f"[!!!] {message}"
        elif log_type == 'ask':
            mssg_to_print = Fore.CYAN + f"[?] {message}"
        else:
            mssg_to_print = Fore.RED + f"[x] {message}"
        #global lines_to_write

        write_log(mssg_to_print + '\n')
        print(mssg_to_print)
        print(Fore.RESET)

# For artificial punch
# /Punch/AddArtificial
# https://apiv3.geovictoria.com/api/

parser = argparse.ArgumentParser("Jud Toolkit v1")
parser.add_argument('-m', dest='module', help="Modulo a ejecutar")
parser.add_argument('-f', dest='file_path', help="Ubicacion del archivo a procesar")
parser.add_argument('-u', dest='api_url', help="URL Api")
parser.add_argument('-t', dest='api_token', help="Token Api (Api bearer)")
parser.add_argument('-k', dest='api_key', help="OAuth Key")
parser.add_argument('-s', dest='api_secret', help="OAuth Secret")
parser.add_argument('-v', dest='verbose', default=True, help="Mostrar detalle completo durante ejecucion")
parser.add_argument('-a', dest='environment', default='sandbox', help="Ambiente (sandbox, produccion)")
parser.add_argument('-c', dest='folder_path', help="Procesar todos los archivos de una carpeta")
parser.add_argument("-n", dest="rows_number", help="Cantidad de registros por archivo")

# Options
max_threads = 1000

# Parse args (args.XXXX)
args = parser.parse_args()
final_uri = ''
api_key = ''
api_secret = ''
api_token = ''

# Const modules
# Add punch
add_punch_module_const = 'add_punch'
add_punch_endpoint = '/Punch/AddArtificial'

# Add Timeoffs
add_timeoff_module_const = 'add_timeoff'
add_timeoff_endpoint = '/TimeOff/Upsert'

# Add Position
add_position_module_const = 'add_position'
add_position_endpoint = '/Position/Add'

# Enable User
enable_user_module_const = 'user_enable'
enable_user_endpoint = '/User/Enable'

# Add user
add_user_module_const = 'add_user'
add_user_endpoint = '/User/Add'

# Add project
add_project_module_const = 'add_project'
add_project_endpoint = '/Project/Add'

# Disable User
disable_user_module_const = 'user_disable'
disable_user_endpoint = '/User/Disable'

# api ripley
ripley_punch_module_const = 'ripleyPunch'
ripley_punch_endpoint = '/Punch/ListPunches'

# Const API
customer_api_sandbox = ''
customer_api_prod = ''
apiv3_api_sandbox = ''
apiv3_api_prod = ''
apiv3_ripley = ''

headers = { 'Content-Type': 'application/json' }

# Const environment
env_sandbox = 'sandbox'
env_prod = 'produccion'
env_ripley = 'ripley'

def handler(signum, frame):
    exit_msg = "Interrupción detectada, ¿Desea terminar la ejecución? [s/n]"
    print(exit_msg, end="", flush=True)
    response = readchar.readchar()
    if response.lower() == 's' or response.lower() == 'y':
        write_log("---INTERRUPCION MANUAL---")
        exit(1)
    else:
        print('', end="\r", flush=True)
        print(' ' * len(exit_msg), end='', flush=True)
        print('  ', end="\r", flush=True)

signal.signal(signal.SIGINT, handler)

def validate_apiv3_basic_info():
    # ApiV3 Validation
    if args.environment:
        # Validate environment to API
        global final_uri
        if args.api_url and args.api_url != "":
            final_uri = args.api_url
        else:
            if args.environment == env_sandbox:
                final_uri = apiv3_api_sandbox
            elif args.environment == env_prod:
                final_uri = apiv3_api_prod
            else:
                print_log('error', "El ambiente especificado no existe", True)
        
        print_log('info', final_uri)

        # Validate connection data to API
        if args.api_key:
            if args.api_secret:
                global api_key
                global api_secret
                api_key = args.api_key
                api_secret = args.api_secret
                print_log('success', "Datos de conexión a la API ingresados")
            else:
                print_log('error', "Se debe ingresar un api secret", True)
        else:
            print_log('error', "Se debe ingresar un api key", True)
    else:
        print_log('error', "Se debe digitar un ambiente", True)

def validate_customerapi_basic_info():
    # CustomerApi Validation
    if args.environment:
        global final_uri
        if args.api_url and args.api_url != "":
            final_uri = args.api_url
        else:
            if args.environment == env_sandbox:
                final_uri = customer_api_sandbox
            elif args.environment == env_prod:
                final_uri = customer_api_prod
            else:
                print_log('error', "El ambiente especificado no existe", True)
        
        print_log('info', final_uri)

        # Validate connection data to API
        if args.api_token:
            global api_token
            api_token = args.api_token
            print_log('success', "Datos de conexión a la API ingresados")
        else:
            print_log('error', "Se debe ingresar el api token", True)
    else:
        print_log('error', "Se debe digitar un ambiente", True)

def validate_apiRipley_basic_info():
    # CustomerApi Validation
    if args.environment:
        global final_uri
        if args.api_url and args.api_url != "":
            final_uri = args.api_url
        else:
            if args.environment == env_sandbox:
                final_uri = customer_api_sandbox
            elif args.environment == env_ripley:
                final_uri = apiv3_ripley
            else:
                print_log('error', "El ambiente especificado no existe", True)
        
        print_log('info', final_uri)

        # Validate connection data to API
        if args.api_token:
            global api_token
            api_token = args.api_token
            print_log('success', "Datos de conexión a la API ingresados")
        else:
            print_log('error', "Se debe ingresar el api token", True)
    else:
        print_log('error', "Se debe digitar un ambiente", True)


def validate_file(extension = ''):
    if args.file_path:
        file_path = args.file_path
        if os.path.exists(file_path):
            filep = Path(file_path)
            if filep.is_file():
                if extension and file_path.endswith(extension):
                    print_log('success', file_path)
                else:
                    print_log('error', f"Formato de archivo erroneo, debe ser ({extension})", True)
            else:
                print_log('error', "La ruta no apunta a un archivo...", True)
        else:
            print_log('error', "La ruta del archivo no fue encontrada", True)
    else:
        print_log('error', "Debe digitar la ubicacion del archivo a procesar", True)

def read_csv():
    df = pd.read_csv(args.file_path, header=0, delimiter=';')
    return df

def apiv3_post(api_url, data):
    oauth = OAuth1(api_key, api_secret)
    print_log('info', final_uri)
    print_log('info', data)
    try:
        request = requests.post(url = api_url, data = json.dumps(data), headers=headers, auth=oauth)
        request.raise_for_status()
        status_code = request.status_code
    
        if status_code == 200:
            print_log('success', f"Agregado correctamente. {request.content}")
        else:
            print_log('error', f"ERROR: ({status_code}) {request.content}")
    except Exception as ex:
        print_log('error', f"ERROR: {str(ex)}")
        print_log('error', f"{request.content}")

def customerapi_post(api_url, data):
    print_log('info', final_uri)
    print_log('info', data)
    try:
        headers['Authorization'] = api_token
        request = requests.post(url = api_url, data = json.dumps(data), headers=headers)
        request.raise_for_status()
        status_code = request.status_code

        if status_code == 200:
            print_log('success', f"Agregado correctamente. {request.content}")
        else:
            print_log('error', f"ERROR: ({status_code}) {request.content}")
            print_log('error', f"{data}")
    except Exception as ex:
        print_log('error', f"ERROR: {str(ex)}")
        print_log('error', f"{request.content}")
    

def ripley_post(api_url, data, text_punch):
    print_log('info', final_uri)
    print_log('info', data)
    text_punch1 = open("marcasErrores2.txt", "a")
    
    list_user = []
    
    try:
        headers['Authorization'] = api_token
        request = requests.post(url = api_url, data = json.dumps(data), headers=headers)
        # request.raise_for_status()
        status_code = request.status_code
        users_list = []
        respuesta = request.text.split(',')
        cont = 0


        if status_code == 200:
            print('respuesta desde la API:')
            print(respuesta)
            notFinal_list = []
            final_list = []
            for i in respuesta:
                #i = i.replace(",")
                print('esto es una i')
                print (i)
                list_user.append(i.split(':'))
            print('lista de datos')
            print(list_user)

            for j in list_user:
                print('esto es una J')
                print(j)
                #j = re.sub("\{|\[|\?|\?|\?","",j)
                idUsuario=j[1]
                notFinal_list.append(idUsuario) 
                print('LISTA NO FINAL')
                print(notFinal_list)
                # date = list_user[cont][1]
                # typePunch = list_user[cont][1]
                print('idididididididid')
                #print(idUsuario,date,typePunch)
                print(idUsuario)
                print (cont)
                cont += 1
                
                if cont == 3:
                    print('GUARDANDO')
                    print(notFinal_list)
                    id_tipo = notFinal_list[2]
                    date = notFinal_list[1]
                    user = notFinal_list[0]
                    print('LO QUE SE VA A GUARDAR')
                    guardando = (id_tipo +'GVIC'+date+date+user+user)
                    print(guardando)
                    text_punch1.write(f"\n {guardando}")
                    #text_punch1.save('marcasErrores.xlsx')
                    final_list.append(notFinal_list)
                    print('RESULTADO')
                    cont = 0
                    print(final_list)
                    notFinal_list.clear()
                
                #text_punch.write(f"\n {i}")
                print('------------------------------')
                #archivo_excel.save('marcas.xlsx')
                
        else:
            print_log('error', f"ERROR: ({status_code}) {request.content}")
            print_log('error', f"{data}")
    except Exception as ex:
        print_log('error', f"ERROR: {str(ex)}")
        print_log('error', f"{request.content}")
    

def add_timeoff_module():
    validate_customerapi_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para agregar permisos")
    df = read_csv()
    timeoff_list = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        user_identifier = df['UserIdentifier'][i]
        timeoff_type_id = df['TimeOffTypeId'][i]
        start_date = df['StartDate'][i]
        end_date = df['EndDate'][i]
        start_time = df['StartTime'][i]
        end_time = df['EndTime'][i]
        description = df['Description'][i]

        if not start_time:
            start_time = '00:00'

        if not end_time:
            end_time = '23:59'

        if not description:
            description = 'API'

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            timeoff_list.append({ 'UserIdentifier': str(user_identifier), 'TimeOffTypeId': str(timeoff_type_id), 'StartDate': str(start_date), 'EndDate': str(end_date), 'CreatedByIdentifier': "000000", 'Description': str(description), 'StartTime': str(start_time), 'EndTime': str(end_time), 'Origin': 'API' })
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Deseas proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")
    
    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in timeoff_list:
                threads.append(executor.submit(customerapi_post(final_uri + add_timeoff_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def add_punch_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para agregar marcas")
    # identifier, date(yyyymmddhhmmss), punch_type
    df = read_csv()
    punch_list = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        identifier = df['identifier'][i]
        date = df['date'][i]
        punch_type = df['punch_type'][i]
                
        if not identifier:
            print_log('error', f"Registro {i} sin identificador")
            row_ok = False

        if not date:
            print_log('error', f"Registro {i} sin fecha")
            row_ok = False

        if not punch_type:
            print_log('error', f"Registro {i} sin tipo marca")
            row_ok = False
        
        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            punch_list.append({ 'identifier': str(identifier), 'date': str(date), 'type': punch_type })
        else:
            bad_rows_counter = bad_rows_counter + 1

    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in punch_list:
                threads.append(executor.submit(apiv3_post(final_uri + add_punch_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def add_position_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para agregar cargos")
    df = read_csv()
    add_position = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        position_desc = df['DESCRIPCION_CARGO'][i]
        priority = df['CARGO_PRIORITARIO'][i]
        criticality = df['CRITICO'][i]
        position_state = df['ESTADO_CARGO'][i]

        if not priority:
            priority = 'false'

        if not criticality:
            criticality = 'false'

        if not position_state:
            position_state = 'enabled'

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            add_position.append({ 'DESCRIPCION_CARGO': str(position_desc), 'CARGO_PRIORITARIO': str(priority), 'CRITICO': str(criticality), 'ESTADO_CARGO': str(position_state)})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in add_position:
                threads.append(executor.submit(apiv3_post(final_uri + add_position_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def enable_user_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para habilitar usuarios")
    df = read_csv()
    user_enable = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        identifier = df['identifier'][i]
        email = df['email'][i]

        if not email:
            email = ''

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            user_enable.append({ 'identifier': str(identifier), 'email': str(email)})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in user_enable:
                threads.append(executor.submit(apiv3_post(final_uri + add_project_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def disable_user_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para habilitar usuarios")
    df = read_csv()
    user_disable = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        identifier = df['identifier'][i]
        email = df['email'][i]

        if not email:
            email = ''

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            user_disable.append({ 'identifier': str(identifier), 'email': str(email)})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in user_disable:
                threads.append(executor.submit(apiv3_post(final_uri + disable_user_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def add_project_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para habilitar usuarios")
    df = read_csv()
    add_project = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        ProjectDescription = df['descripcion'][i]
        ProjectAddress = df['direccion'][i]

        if not email:
            email = ''

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            add_project.append({ 'ProjectDescription': str(ProjectDescription), 'email': str(email)})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for data in add_project:
                threads.append(executor.submit(apiv3_post(final_uri + enable_user_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)


def add_user_module():
    validate_apiv3_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para agregar usuarios")
    df = read_csv()
    add_user = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        Identifier = df['Identificador'][i]
        Email = df['Correo Personal'][i]
        Name = df['Nombre'][i]
        LastName = df['Apellido'][i]
        ContractDate = df['Fecha Contrato'][i]
        positionIdentifier = df['Cargo'][i]
        Adress = df['Dirección'][i]
        GroupIdentifier = df['Grupo'][i]

        # if not email:
        #     email = ''

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            add_user.append({ 'Identifier': str(Identifier), 'Email': str(Email), 'Name':str(Name), 'LastName':str(LastName), 'ContractDate':str(ContractDate), 'positionIdentifier':str(positionIdentifier), 'Adress':str(Adress), 'GroupIdentifier':str(GroupIdentifier)})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")

    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            for data in add_user:
                threads.append(executor.submit(apiv3_post(final_uri + add_user_endpoint, data)))
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def split_file():
    validate_file()
    print_log('info', "Iniciando proceso para partir archivo")
    # TODO: Validate -n max_rows_per_file

def ripleyPunch():
    contador = 0
    validate_apiRipley_basic_info()
    validate_file('.csv')
    print_log('info', "Iniciando proceso para agregar usuarios")
    df = read_csv()
    add_user = []
    bad_rows_counter = 0
    ok_rows_counter = 0
    for i in df.index:
        row_ok = True
        StartDate = df['StartDate'][i]
        EndDate = df['EndDate'][i]
        UserIds = df['UserIds'][i]

        # if not email:
        #     email = ''

        if row_ok:
            ok_rows_counter = ok_rows_counter + 1
            add_user.append({ 'StartDate': str(StartDate), 'EndDate': str(EndDate), 'UserIds':str(UserIds).replace("a","")})
        else:
            bad_rows_counter = bad_rows_counter + 1
    print_log('warning', f"Rows to process {ok_rows_counter}")
    print_log('warning', f"Rows to ignore(with errors) {bad_rows_counter}")

    # Ask to proceed
    print_log('ask', f"Seran procesados un total de {ok_rows_counter} ¿Desea proceder? [s/n]: ")
    proceed = input("[s/n]#-> ")
    #archivo_excel = load_workbook('marcas2.xlsx')
    #hoja = archivo_excel['Hoja1']
    text_punch = open("marcasErrores2.txt", "a")
    # pool = multiprocessing.Pool(processes=4)
    # pool.map(ripley_post,(final_uri + ripley_punch_endpoint, add_user,text_punch) )
    if proceed.lower() == 's' or proceed.lower() == 'n':
        # Validate if multithread
        threads = []
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            for data in add_user:
                threads.append([executor.submit(ripley_post(final_uri + ripley_punch_endpoint, data,text_punch))])
                contador =contador + 1
                print(contador)
    else:
        print_log('warning', "No se realizaron modificaciones ni consultas a la API, terminando proceso...")
        exit(1)

def handle_args():
    if args.module:
        print_log('info', f"Modulo a ejecutar {args.module}")

        if args.module == add_punch_module_const:
            add_punch_module()
        elif args.module == add_timeoff_module_const:
            add_timeoff_module()
        elif args.module == add_position_module_const:
            add_position_module()
        elif args.module == enable_user_module_const:
            enable_user_module()
        elif args.module == disable_user_module_const:
            disable_user_module()
        elif args.module == add_project_module_const:
            add_project_module()
        elif args.module == add_user_module_const:
            add_user_module()
        elif args.module == ripley_punch_module_const:
            ripleyPunch()
        else:
            print_log('error', "Este modulo no existe")
            return False
    else:
        print_log('error', "[x] Se debe indicar un modulo a ejecutar")
        return False

handle_args()
