import sys
import argparse
import os.path
import hashlib
import json
import urllib.parse
from urllib.request import urlopen
from urllib.error import HTTPError
from halo import Halo

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    NORMAL = '\033[m'

account_id = '[FACEBOOK-BUSINESS-ACCOUNT-ID]'
access_token = '[FACEBOOK-ACCESS-TOKEN]'
app_id = ['[FACEBOOK-APPLICATION-ID]']

accepted_fields =["EXTERN_ID","EMAIL","PHONE","GEN","DOB","AGE","UID","DOBY","DOBM","DOBD","LN","FN","FI","CT","ST","ZIP","MADID","COUNTRY"]
chunk_size = 10000

class audience:
    name = ""
    description = ""
    id = ""
    records = 0
    records_received = 0
    records_invalid = 0
    fields = []
    hash_map = []


def parse_header(f):
    first_line = f.readline()
    attributes = [x.strip() for x in first_line.split(',')]
    for i, item in enumerate(attributes):
        item = str(item).upper()
        audience.fields.append(item)
        if item not in accepted_fields:
            print(bcolors.FAIL+"ERROR: "+bcolors.NORMAL+"The field "+item+" is not accepted")
            sys.exit(-1)

def send_audience(chunk):
    spinner = Halo(text='Sending chunk', spinner='dots')
    spinner.start()
    payload = {
        'schema' : audience.fields,
        'app_ids' : app_id,
        'data' : chunk
    }
    data = {
        "payload" : payload,
        "access_token" : access_token
    }
    
    data = urllib.parse.urlencode(data)
    data = data.encode('ascii')
    url = "https://graph.facebook.com/v3.2/"+audience.id+"/users"
    try:
        with urlopen(url,data) as response: 
            info = response.read().decode('ASCII')
        response_data = json.loads(str(info))
        spinner.succeed("Chunk was sent")
        audience.records_received += response_data['num_received']
        audience.records_invalid += response_data['num_invalid_entries']
    except HTTPError as e:
        spinner.fail("Chunk failed")

def get_audience_id():
    print('\nCreate Audience:')
    spinner = Halo(text='Creating audience', spinner='dots')
    spinner.start()
    values = {
        'name' : audience.name,
        'description' : audience.description,
        'subtype' : 'CUSTOM',
        'customer_file_source' : 'USER_PROVIDED_ONLY',
        'access_token' : access_token
    }
    data = urllib.parse.urlencode(values)
    data = data.encode('ascii')
    try:
        with urlopen("https://graph.facebook.com/v3.2/act_"+account_id+"/customaudiences",data) as response:
            info = response.read().decode('ASCII')
        spinner.succeed("Audience was created")
        response_data = json.loads(str(info))
    except HTTPError as e:
        spinner.fail("Audience creation failed")
        response = e.read()
        print(response)
        sys.exit(-1)
    return response_data['id']

def open_file(the_file):
    try:
       fp = open(the_file, 'r')
       return fp
    except FileNotFoundError:
        print(bcolors.FAIL+"ERROR:"+bcolors.NORMAL+" The file "+the_file+" could not be opened")
        sys.exit(1)

def process_file(fp):
    print("\nProcessing file: "+fp.name)
    i = 0
    chunk = []
    for line in fp:
        row = []
        data = [x.strip() for x in line.split(',')]
        for j, field in enumerate(data):
            field = str(field).lower()
            if (field != ""):
                field = hashlib.sha256(str(field).encode('utf-8')).hexdigest()
            row.append(field)
        chunk.append(row)
        audience.records += 1
        i += 1
        if (i >= chunk_size):
            send_audience(chunk)
            chunk = []
            i = 0
    if (i != 0):
        send_audience(chunk)
    print("Finished processing file")
    
def create_audience(fp):
    parse_header(fp)
    audience.name = input("Please enter an audience name: ")
    audience.description = input("Please enter a description for this audience: ")
    audience.id = get_audience_id()
    process_file(fp)
    print("\n"+bcolors.UNDERLINE+"Audience ("+audience.name+") Upload Summary:"+bcolors.NORMAL)
    print("Total "+str(audience.records)+" records in file")
    print("Total "+str(audience.records_received)+" records received")
    print("Total "+str(audience.records_invalid)+" records were invalid")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Manage Facebook audiences')
    argument_group = parser.add_mutually_exclusive_group(required=True)
    argument_group.add_argument("--create", dest="create",
        help="create new Facebook Audience from file", metavar="FILE",
        type=lambda x: open_file(x))
    return parser.parse_args()

def main() :
    print(bcolors.UNDERLINE+"Facebook Audience Import Tool"+bcolors.NORMAL+"\n")
    args = parse_arguments()
    if args.create is not None:
        create_audience(args.create)
    
if __name__ == "__main__":main()