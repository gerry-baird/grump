import requests
from requests.auth import HTTPBasicAuth
import csv
import logging
from tqdm import tqdm
import time
import sys
import warnings
import aiohttp
import asyncio
from time import sleep
from jsonpath_ng import jsonpath, parse
import yaml
from yaml.loader import SafeLoader
import argparse



parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="config file name")
args = parser.parse_args()
config_filename = args.config

if config_filename is None:
    # Default config filename
    config_filename = "config.yaml"



# Setup Logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
file_handler = logging.FileHandler('grump.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

# Load the config
try:
    with open(config_filename, 'r') as config_file:
        config = yaml.load(config_file, Loader=SafeLoader)
except FileNotFoundError as e:
    msg = "Cannot find config file : config.yaml or the config file specified in arguments"
    print(msg)
    logger.error(msg)
    sys.exit("Grump aborting.")

# Apply config to global variables
ROOT_URL = config['root-url']
PROCESS_SEARCH_URL = "rest/bpm/wle/v1/processes/search?"
PROCESS_SEARCH_BPD_FILTER = "searchFilter="
PROCESS_SEARCH_PROJECT_FILTER = "&projectFilter="
TASK_SUMMARY_URL = "rest/bpm/wle/v1/process/"
TASK_SUMMARY_URL_SUFFIX = "/taskSummary/"
TASK_DETAIL_URL = "rest/bpm/wle/v1/task/"
TASK_DETAIL_URL_SUFFIX = "?parts=data"
PROJECT_ACRONYM = config['project']
PROCESS_NAME = config['process-name']
USER = config['user']
PWD = config['password']
THREAD_COUNT = config['thread-count']
AUTH_DATA = HTTPBasicAuth(USER, PWD)
INSTANCE_LIMIT = config['instance-limit']
MODIFIED_AFTER = config['modified-after']
MODIFIED_BEFORE = config['modified-before']
BUSINESS_DATA = config['business-data']


def invalid_number(number):
    invalid = False
    try:
        num = int(number)
    except ValueError:
        invalid = True

    return invalid


def build_instance_search_url():

    url = ROOT_URL + PROCESS_SEARCH_URL

    # If modified after given in config add it here
    if MODIFIED_AFTER is not None:
        modified_after_str = MODIFIED_AFTER.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = url + "modifiedAfter=" + modified_after_str + "&"

    # If modified after given in config add it here
    if MODIFIED_BEFORE is not None:
        modified_before_str = MODIFIED_BEFORE.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = url + "modifiedBefore=" + modified_before_str + "&"

    # Add the process name and project to the URL
    url = url + PROCESS_NAME + PROCESS_SEARCH_PROJECT_FILTER + PROJECT_ACRONYM
    #url = url + PROCESS_NAME
    
    if INSTANCE_LIMIT is not None and INSTANCE_LIMIT > 0:
        url = url + f"&limit={INSTANCE_LIMIT}"

    return url


def get_instance_list():
    instance_list = []
    try:
        url = build_instance_search_url()
        response = requests.get(url, auth=AUTH_DATA, verify=False)
        status = response.status_code

        if status == 200:
            instance_data_json = response.json()
            # logger.debug(json.dumps(instance_data_json))


            for bpd_instance in instance_data_json['data']['processes']:
                instance_list.append(bpd_instance['piid'])

            return instance_list

    except Exception as e:
        message = f"Unexpected error processing BPD : {PROCESS_NAME}"
        logger.error(message)
        logger.error(e)

# Function to fetch task details for task id's in the task_list
async def get_task_details(session, instance, task_list, event_data, pbar):

    auth = aiohttp.BasicAuth(login=USER, password=PWD, encoding='utf-8')

    for task_id in task_list:
        URL = ROOT_URL + TASK_DETAIL_URL + task_id + TASK_DETAIL_URL_SUFFIX
        logger.debug(f"Fetching task details for bpd instance : {instance} and Task : {task_id}")
        async with session.get(URL, auth=auth, ssl=False) as task_detail_response:
            task_detail_status = task_detail_response.status
            if task_detail_status == 200:
                task_detail_data = await task_detail_response.json()

                task_name = task_detail_data['data']['name']
                task_start = task_detail_data['data']['startTime']
                task_completion = task_detail_data['data']['completionTime']
                task_team = task_detail_data['data']['teamDisplayName']
                task_owner = task_detail_data['data']['owner']

                # Basic info for IPM
                event = {'processID': instance, 'taskID': task_id,'taskName': task_name, 'startTime': task_start,
                         'endTime': task_completion, 'team': task_team, 'owner': task_owner}

                # Add any optional variables
                if BUSINESS_DATA is not None:
                    for task_variable in BUSINESS_DATA:
                        variable_name = task_variable['name']
                        variable_path = task_variable['path']

                        # Default value to use if no match is found in the task data
                        variable_value = ""

                        jsonpath_expression = parse(variable_path)
                        for match in jsonpath_expression.find(task_detail_data):
                            # Update the default variable
                            variable_value = match.value
                            break

                        # Add the value to the event dictionary
                        event[variable_name] = variable_value

                # Update the even_data list passed from the calling function
                event_data.append(event)
                pbar.update(1)

# Function to fetch task id's for a specific process instance
async def get_task_summaries(session, instance, bpd_instance_dict, pbar):

    logger.debug('Fetching task summaries for bpd instance : ' + instance)
    URL = ROOT_URL + TASK_SUMMARY_URL + instance + TASK_SUMMARY_URL_SUFFIX
    auth = aiohttp.BasicAuth(login=USER, password=PWD, encoding='utf-8')

    async with session.get(URL, auth=auth, ssl=False) as task_summary_response:
        task_summary_status = task_summary_response.status
        if task_summary_status == 200:
            task_summary_data = await task_summary_response.json()

            task_list = []
            for task_summary in task_summary_data['data']['tasks']:
                task_id = task_summary['tkiid']
                logger.debug(f"Instance {instance} found Task : {task_id}")
                task_list.append(task_id)

            # We have the instance + a list of its task id's
            # Update the bpd_instance_dict that was passed from the calling function
            bpd_instance_dict[instance] = task_list
            pbar.update(1)



async def get_instance_data(instance_list, event_data):
    # Dictionary to hold bpd instances and related tasks
    bpd_instance_dict = {}

    instance_count = len(instance_list)
    print(f"Processing {instance_count} instances. Fetching task summaries .....")
    # Initialise the connector
    connector = aiohttp.TCPConnector(limit=THREAD_COUNT)

    # create a ClientTimeout to allow for long running jobs
    infinite_timeout = aiohttp.ClientTimeout(total=None , connect=None,
                          sock_connect=None, sock_read=None)

    # Get the task list for each instance
    async with aiohttp.ClientSession(connector=connector, timeout=infinite_timeout) as session:
        async_tasks = []
        pbar = tqdm(total=instance_count)
        for instance in instance_list:
            async_task = asyncio.ensure_future(get_task_summaries(session, instance, bpd_instance_dict, pbar))
            async_tasks.append(async_task)

        await asyncio.gather(*async_tasks)
        pbar.close()

    # Re-initialise the connector
    connector = aiohttp.TCPConnector(limit=THREAD_COUNT)

    # At this point the bpd_instance_dict variable will be populated as it is passed as a parameter
    # to get_task_summaries() and updated each time we fetch the tasks associated with a process instance

    # Calculate how many tasks exist in the dictionary
    task_count = 0
    for key, value in bpd_instance_dict.items():
        if isinstance(value, list):
            task_count += len(value)
    print(f"Processing {task_count} tasks. Fetching task details .....")
    logger.info(f"Processing {task_count} tasks .....")

    #Just for console formatting
    sleep(0.25)

    infinite_timeout = aiohttp.ClientTimeout(total=None, connect=None,
                                             sock_connect=None, sock_read=None)
    async with aiohttp.ClientSession(connector=connector, timeout=infinite_timeout) as session:
        async_tasks = []

        pbar = tqdm(total=task_count)
        for instance in instance_list:
            task_list = bpd_instance_dict[instance]

            async_task = asyncio.ensure_future(get_task_details(session, instance, task_list, event_data, pbar))
            async_tasks.append(async_task)

        await asyncio.gather(*async_tasks)
        pbar.close()

def main():
    instance_list = []
    event_data = []
    start_time = time.time()

    # Supress warnings for unsigned SSL certs
    if not sys.warnoptions:
        warnings.simplefilter("ignore")

    logger.info('GRUMP : Starting')
    print('GRUMP : Starting')

    # Validate config
    valid_config = True
    msg_list = ['GRUMP : Invalid Config']

    if invalid_number(THREAD_COUNT):
        msg_list.append('thread-count is invalid')
        valid_config = False

    if not valid_config :
        for msg in msg_list:

            logger.info(msg)
            print(msg)

        sys.exit("Grump aborting.")



    instance_list = get_instance_list()
    print(f"Found : {len(instance_list)} instances of BPD {PROCESS_NAME} in project {PROJECT_ACRONYM}")
    logger.info(f"Found : {len(instance_list)} instances of BPD {PROCESS_NAME} in project {PROJECT_ACRONYM}")

    # get_instance_data() calls get_task_summaries() then get_task_details()
    asyncio.run(get_instance_data(instance_list, event_data))

    data_file = open('data_file.csv', 'w')
    csv_writer = csv.writer(data_file)
    header = ['processID', 'taskID','Task Name', 'Start', 'End', 'Team', 'Owner']

    # Now add the variables to the header
    if BUSINESS_DATA is not None:
        for task_variable in BUSINESS_DATA:
            variable_name = task_variable['name']
            header.append(variable_name)

    row_count = 0
    for record in event_data:
        if row_count == 0:
            csv_writer.writerow(header)
            row_count += 1

        csv_writer.writerow(record.values())

    data_file.close()

    print("GRUMP Finished")
    print("--- %s seconds ---" % (time.time() - start_time))
    logger.info('GRUMP Finished')


if __name__ == "__main__":
    main()




