import boto3
import pickle
from shutil import copy
import config

aws_connect = boto3.client('logs')
LogStreamData_Raw = {}
LogStreamData_old = {}
Download_LogFiles = {}
Stream_dump_metadata_files = {}
LogStreamName = config.CLOUDWATCH_STREAMNAME


def write_to_file(filename, payload):
    """This function will write the given
    data to the given file"""
    with open(filename, 'ab') as temp_file:
        temp_file.write(payload)
    return None


def dump_values(filename, flag, payload=None):
    """This will dump and fetch
    the key values to/from a file"""
    global LogStreamData_old
    if flag == 'DUMP':
        with open(filename,'wb') as dump_file:
            pickle.dump(payload,dump_file)
    elif flag == 'PICK':
        with open(filename,'rb') as fetch_file:
            LogStreamData_old = pickle.load(fetch_file)
    return LogStreamData_old if (flag == 'PICK') else None


def make_backup_of_dump_files(filename):
    file = '''{fn}.{ext}'''
    for numbers in range(5,0,-1):
        copy(file.format(fn=filename, ext=str(numbers-1)),file.format(fn=filename, ext=str(numbers-1)))
    copy(filename,file.format(fn=filename, ext='0'))
    return None


def describe_stream(stream_name):
    """This function will describe the
    log stream to filter the new data"""
    global LogStreamData_Raw
    try:
        LogStreamData_Raw = aws_connect.describe_log_streams(logGroupName=stream_name,
                                                    orderBy='LastEventTime', descending=True, limit=50)
    except Exception as excp:
        print(excp)
    return LogStreamData_Raw['logStreams']


def format_logstreamdata(payload):
    """This function will format the logstream data
    by adding the filename as a key to its metadata"""
    LogStreamData_old_modified = {}
    for data in payload:
        LogStreamData_old_modified[data['logStreamName']] = data
    return LogStreamData_old_modified


def check_for_new_logs(OldStreamData, NewStreamData):
    """
    Flags for log file:
    NLF - Newly created log file.
    OLFA - Old log file has been appended with new data
    """
    global Download_LogFiles
    OldLogFiles = OldStreamData.keys()
    NewLogFIles = NewStreamData.keys()
    for newfiles in list(set(NewLogFIles) - set(OldLogFiles)):
        Download_LogFiles[newfiles] = 'NLF'
    for logfiles in NewStreamData:
        try:
            if NewStreamData[logfiles]['lastIngestionTime'] != OldStreamData[logfiles]['lastIngestionTime']:
                Download_LogFiles[logfiles] = 'OLFA'
        except Exception as excp:
            print excp
    return Download_LogFiles


def download_log_events(loggroupname, logstreamname,flag,token):
    try:
        raw_logs = aws_connect.get_log_events(logGroupName=loggroupname, logStreamName=logstreamname,
                                              startFromHead=flag, nextToken=token)
    except Exception as excp:
        print excp


def log_to_file(filename,payload):
    try:
        with open(filename, 'ab') as log_file:
            log_file.write(payload)
    except Exception as excp:
        print excp


for Streams in LogStreamName:
    Streams_described = describe_stream(Streams)
    formated_logstreamdata = format_logstreamdata(payload=Streams_described)
    Loaded_Streamdata = dump_values(filename=config.dump_matadata_files(Streams)[0], flag='PICK')
    print(Loaded_Streamdata)
    print(formated_logstreamdata)
    check_for_new_logs(OldStreamData=Loaded_Streamdata, NewStreamData=formated_logstreamdata)
    print Download_LogFiles