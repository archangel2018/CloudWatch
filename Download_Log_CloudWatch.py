import boto3
import pickle
import time
from shutil import copy
import config
from subprocess import PIPE, Popen
import logging
FORMAT = '%(asctime) %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(filename='/opt/cloudwatch-scripts/Download_Log_CloudWatch.log', level=logging.INFO, filemode='a',
                             format='%(asctime)s,%(msecs)d %(name)s - %(levelname)s %(message)s',
                             datefmt="%Y-%m-%d %H:%M:%S")

logging.getLogger('boto').setLevel(logging.CRITICAL)
'''Assigning variable here that are used inside DEF's and loops'''
global_sleep_time = 15
aws_connect = boto3.client('logs')
LogGroupData_Raw = {}
Data_old = {}
Download_LogStreamFiles = {}
Stream_dump_metadata_files = {}
LogGroupName = config.CLOUDWATCH_GROUPNAME
Dump_LogStreamData = {}
Dump_LogGroupdata = {}
make_logfile_for_kibana = []
cmd_grep_raw = '''cat {filename_in} | grep -vi "performance_schema" | grep -vi INFORMATION_SCHEMA  |
grep -vi "mysql-connector-java" | grep -vi 'Your log message was truncated' | grep -v '^$' >> {filename_out} '''


def dump_values(filename, flag, payload=None):
    """
    This will dump and fetch
    the key values to/from a given file.
    """
    logging.info('Entering DUMP/PICK block')
    global Data_old
    if flag == 'DUMP':
        logging.info('Dumping the provided payload in file %s', filename)
        with open(filename, 'wb') as dump_file:
            pickle.dump(payload, dump_file)
            dump_file.close()
    elif flag == 'PICK':
        logging.info('Picking the payload from the provided file: %s', filename)
        with open(filename, 'rb') as fetch_file:
            Data_old = pickle.load(fetch_file)
            fetch_file.close()
    return Data_old if (flag == 'PICK') else None


def make_backup_of_dump_files(filename):
    """
    This function will ensure that the metadata dump file
    has a retention of 5 last backup's.
    """
    logging.info('Rotating the metadata file')
    file = '''{fn}.{ext}'''
    for numbers in range(5, 0, -1):
        copy(file.format(fn=filename, ext=str(numbers-1)), file.format(fn=filename, ext=str(numbers-1)))
    copy(filename, file.format(fn=filename, ext='0'))
    return None


def describe_group(group_name):
    """
    This function will describe the
    log stream to filter the new data.
    :param group_name: Name of the steam
    :return: Log steam meta data values i.e logfile name , last modified time.
    """
    global LogGroupData_Raw
    logging.info('Describing the Log group')
    try:
        time.sleep(global_sleep_time)
        LogGroupData_Raw = aws_connect.describe_log_streams(logGroupName=group_name,orderBy='LastEventTime', descending=True, limit=50)
    except Exception as excp:
        logging.error('Error while fetching log group data')
        logging.exception(excp)
        exit()
    return LogGroupData_Raw['logStreams']


def format_loggroupdata(payload):
    """
    This function will format the logstream data
    by adding the filename as a key to its metadata.
    :param payload: Current logstream metadata
    :return:
    """
    logging.info('Formatting the provided data')
    LogGroupData_old_modified = {}
    for data in payload:
        LogGroupData_old_modified[data['logStreamName']] = data
    return LogGroupData_old_modified


def check_for_new_logs(OldGroupData, NewGroupData):
    """
    Flags for log file:
    NLF - Newly created log file.
    OLFA - Old log file has been appended with new data.
    :param OldGroupData: Pass the Previous stream metadata
    :param NewGroupData: Pass the new stream metadata
    :return: This function will return the status of log files that needs to downloaded
    """
    logging.info('Comparing the log group data')
    global Download_LogStreamFiles
    OldLogFiles = OldGroupData.keys()
    NewLogFIles = NewGroupData.keys()
    for newfiles in list(set(NewLogFIles) - set(OldLogFiles)):
        Download_LogStreamFiles[newfiles] = 'NLF'
    for rm_keys in Download_LogStreamFiles:
        if Download_LogStreamFiles[rm_keys] == 'NLF':
            del NewGroupData[rm_keys]
    for logfiles in NewGroupData:
        print (logfiles)
        try:
            if NewGroupData[logfiles]['lastIngestionTime'] != OldGroupData[logfiles]['lastIngestionTime']:
                Download_LogStreamFiles[logfiles] = 'OLFA'
        except Exception as excp:
            logging.error('Error while comparing the old and new data')
            logging.exception(excp)
            exit()
    return Download_LogStreamFiles


def log_to_file(filename, payload):
    """
    This function is write the provided data to a given file.
    :param filename: filename where the logs should be saved.
    :param payload: Logs from cloudwatch
    :return: Nothing
    """
    try:
        with open(filename, 'ab') as log_file:
            for mssg in payload:
                log_file.write(str(mssg['message'].encode('utf-8')+'\n'))
            log_file.close()
    except Exception as excp:
        logging.exception(excp)


def execute_shell_commands(cmd):
    """
    !!**CAUTION**!! This function will execute any provided shell commands.
    :param cmd: Provide the command to execute
    :return: Return's the output of the command
    """
    output = ()
    try:
        exec_me = Popen(cmd, shell=True, stdout=PIPE)
        output = exec_me.communicate()
    except Exception as excp:
        logging.error(excp)
    return output

logging.info('''Log download process starts here !!''')
for Groups in LogGroupName:
    formated_loggroupdataTemp = {}
    Groups_described = describe_group(Groups)
    formated_loggroupdata = format_loggroupdata(payload=Groups_described)
    formated_loggroupdataTemp.update(formated_loggroupdata)
    Loaded_LogGroupdata = dump_values(filename=config.dump_metadata_files(Groups)[0], flag='PICK')
    Loaded_LogStreamData = dump_values(filename=config.dump_metadata_files(Groups)[1], flag='PICK')
    check_for_new_logs(OldGroupData=Loaded_LogGroupdata, NewGroupData=formated_loggroupdataTemp)
    Dump_LogStreamData = Loaded_LogStreamData
    print(Download_LogStreamFiles)
    try:
        for LogStreamfiles in Download_LogStreamFiles:
            '''This statement will be executed if the log file is newly generated'''
            if Download_LogStreamFiles[LogStreamfiles] == 'NLF':
                '''creating a empty key in the dict for the new logfile'''
                Loaded_LogStreamData[LogStreamfiles] = {}
                Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = '0'
                Tailflag = True
                '''This while loop will run until the end of the log file is downloaded'''
                while True:
                    Next_Token = Loaded_LogStreamData[LogStreamfiles]['nextForwardToken']
                    logging.info("Initial Token value %s" ,Next_Token)
                    time.sleep(global_sleep_time)
                    raw_logs = {}
                    if Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] == '0':
                        raw_logs = aws_connect.get_log_events(logGroupName=Groups, logStreamName=LogStreamfiles,
                                                              startFromHead=Tailflag)
                    else:
                        raw_logs = aws_connect.get_log_events(logGroupName=Groups, logStreamName=LogStreamfiles,nextToken=Next_Token)
                    if raw_logs['events']:
                        log_to_file(config.dump_metadata_files(Groups)[2], raw_logs['events'])
                    '''Setting the Tailflag variable to False as first iteration would have downloaded the first 10000
                    lines of the file(Till 1 mb)'''
                    Tailflag = False
                    '''Terminate the while loop if the lasttoken and current token matches'''
                    if Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] == raw_logs['nextForwardToken']:
                        del raw_logs['events']
                        del raw_logs['ResponseMetadata']
                        Dump_LogStreamData[LogStreamfiles] = raw_logs
                        logging.info("Current stream Token is %s",
                                     Dump_LogStreamData[LogStreamfiles]['nextForwardToken'])
                        make_logfile_for_kibana.append(Groups)
                        break
                    else:
                        Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = raw_logs['nextForwardToken']
            elif Download_LogStreamFiles[LogStreamfiles] == 'OLFA':
                while True:
                    Next_Token = Loaded_LogStreamData[LogStreamfiles]['nextForwardToken']
                    logging.info("Initial Token value %s", Next_Token)
                    logging.info("Downloadin logs for file %s %s", LogStreamfiles, LogStreamfiles)
                    time.sleep(global_sleep_time)
                    raw_logs = {}
                    try:
                        raw_logs = aws_connect.get_log_events(logGroupName=Groups, logStreamName=LogStreamfiles,
                                                              nextToken=Next_Token)
                    except Exception as e:
                        logging.exception(e)
                    logging.info(raw_logs['ResponseMetadata'])
                    if raw_logs['events']:
                        log_to_file(config.dump_metadata_files(Groups)[2], raw_logs['events'])
                    if Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] == raw_logs['nextForwardToken']:
                        logging.info("Entering IF block")
                        del raw_logs['events']
                        del raw_logs['ResponseMetadata']
                        Dump_LogStreamData[LogStreamfiles] = raw_logs
                        logging.info("Current stream Token is %s",
                                    Dump_LogStreamData[LogStreamfiles]['nextForwardToken'])
                        make_logfile_for_kibana.append(Groups)
                        break
                    else:
                        Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = raw_logs['nextForwardToken']
            elif Download_LogStreamFiles[LogStreamfiles] == 'Done':
                logging.info("Already downloaded")
            Download_LogStreamFiles[LogStreamfiles] = 'Done'
    except Exception as excp:
        logging.exception(excp)
    logging.info(Dump_LogStreamData.keys())
    logging.info(formated_loggroupdata.keys())
    dump_values(filename=config.dump_metadata_files(Groups)[1], flag='DUMP', payload=Dump_LogStreamData)
    dump_values(filename=config.dump_metadata_files(Groups)[0], flag='DUMP', payload=formated_loggroupdata)
    Dump_LogStreamData = {}
    Dump_LogGroupdata = {}
    formated_loggroupdata = {}


logging.info(make_logfile_for_kibana)
for downloadedfiles in make_logfile_for_kibana:
    cmd_grep = cmd_grep_raw.format(filename_in=config.dump_metadata_files(downloadedfiles)[2],
                                   filename_out=config.dump_metadata_files(downloadedfiles)[3])
    cmd_output = execute_shell_commands(cmd_grep)[1]
    if cmd_output != 'None':
        open(config.dump_metadata_files(downloadedfiles)[2], 'w').close()
    else:
        logging.info('Error while executing command')
        logging.info('cmd_output %s', cmd_output)
