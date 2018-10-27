import boto3
import pickle
from shutil import copy

aws_connect = boto3.client('logs')
LogStreamData_Raw = {}
LogStreamData_old = {}
LogStreamData_old_modified = {}
Download_LogFiles = {}
Stream_dump_metadata_files = {}


'''This function will write the given data to the given file'''
def write_to_file(filename,payload):
    with open(filename,'ab') as temp_file:
        temp_file.write(payload)
    return None


'''This will dump and fetch the key values to/from a file'''
def dump_values(filename,payload,flag):
    global LogStreamData_old
    if flag == 'DUMP':
        with open(filename,'wb') as dump_file:
            pickle.dump(payload,dump_file)
    elif flag == 'PICK':
        with open(filename,'rb') as fetch_file:
            LogStreamData_old = pickle.load(fetch_file)
    return None

def make_backup_of_dump_files(filename):
    file = '''{fn}.{ext}'''
    for numbers in range(5,0,-1):
        copy(file.format(fn=filename, ext=str(numbers-1)),file.format(fn=filename, ext=str(numbers-1)))
    copy(filename,file.format(fn=filename, ext='0'))
    return None

'''This function will describe the log stream to filter the new data'''
def describe_stream(stream_name):
    global LogStreamData_Raw
    try:
        LogStreamData_Raw = aws_connect.describe_log_streams(logGroupName=stream_name,
                                                    orderBy='LastEventTime', descending=True, limit=50)
    except Exception as excp:
        print(excp)
    return LogStreamData_Raw['logStreams']


'''This function will format the logstream data by adding the filename as a key to its metadata'''
def format_logstreamdata(payload):
    for data in payload:
        LogStreamData_old_modified[data['logStreamName']] = data
    return LogStreamData_old_modified

'''
Flags for log file:
NLF - Newly created log file.
OLFA - Old log file has been appended with new data
'''

def check_for_new_logs(OldStreamData, NewStreamData):
    global Download_LogFiles
    OldLogFiles = OldStreamData.keys()
    NewLogFIles = NewStreamData.Key()
    for newfiles in list(set(NewLogFIles) - set(OldLogFiles)):
        Download_LogFiles[newfiles] = 'NLF'
    for logfiles in NewStreamData:
        try:
            if NewStreamData[logfiles]['lastIngestionTime'] != OldStreamData[logfiles]['lastIngestionTime']:
                Download_LogFiles[logfiles] = 'OLFA'
        except Exception as excp:
            print excp
    return Download_LogFiles

