import boto3
import pickle
from shutil import copy
import config
from subprocess import PIPE, Popen

'''Assigning variable here that are used inside DEF's and loops'''
aws_connect = boto3.client('logs')
LogStreamData_Raw = {}
LogStreamData_old = {}
Download_LogFiles = {}
Stream_dump_metadata_files = {}
LogStreamName = config.CLOUDWATCH_STREAMNAME
Dump_LogData = {}
Dump_Streamdata = {}
make_logfile_for_kibana = []
cmd_grep_raw = '''cat {filename_in} | grep -vi "performance_schema" | grep -vi INFORMATION_SCHEMA  | 
grep -vi "mysql-connector-java" | grep -vi 'Your log message was truncated' | grep -v '^$' >> {filename_out} '''


def dump_values(filename, flag, payload=None):
    """
    This will dump and fetch
    the key values to/from a given file.
    """
    global LogStreamData_old
    if flag == 'DUMP':
        with open(filename, 'wb') as dump_file:
            pickle.dump(payload, dump_file)
            dump_file.close()
    elif flag == 'PICK':
        with open(filename, 'rb') as fetch_file:
            LogStreamData_old = pickle.load(fetch_file)
            fetch_file.close()
    return LogStreamData_old if (flag == 'PICK') else None


def make_backup_of_dump_files(filename):
    """
    This function will ensure that the metadata dump file
    has a retention of 5 last backup's.
    """
    file = '''{fn}.{ext}'''
    for numbers in range(5, 0, -1):
        copy(file.format(fn=filename, ext=str(numbers-1)), file.format(fn=filename, ext=str(numbers-1)))
    copy(filename, file.format(fn=filename, ext='0'))
    return None


def describe_stream(stream_name):
    """
    This function will describe the
    log stream to filter the new data.
    """
    global LogStreamData_Raw
    try:
        LogStreamData_Raw = aws_connect.describe_log_streams(logGroupName=stream_name,
                                                             orderBy='LastEventTime', descending=True, limit=50)
    except Exception as excp:
        print(excp)
    return LogStreamData_Raw['logStreams']


def format_logstreamdata(payload):
    """
    This function will format the logstream data
    by adding the filename as a key to its metadata.
    """
    LogStreamData_old_modified = {}
    for data in payload:
        LogStreamData_old_modified[data['logStreamName']] = data
    return LogStreamData_old_modified


def check_for_new_logs(OldStreamData, NewStreamData):
    """
    Flags for log file:
    NLF - Newly created log file.
    OLFA - Old log file has been appended with new data.
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
            print(excp)
    return Download_LogFiles


def log_to_file(filename, payload):
    """
    This function is write the provided data to a given file.
    :param filename: filename where the logs should be saved.
    :param payload: Logs from cloudwatch
    :return: Nothing
    """
    try:
        with open(filename, 'ab') as log_file:
            log_file.write(payload)
            log_file.close()
    except Exception as excp:
        print(excp)


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
        print(excp)
    return output


'''Log download process starts here !!'''
for Streams in LogStreamName:
    Streams_described = describe_stream(Streams)
    formated_logstreamdata = format_logstreamdata(payload=Streams_described)
    Loaded_Streamdata = dump_values(filename=config.dump_metadata_files(Streams)[0], flag='PICK')
    Loaded_LogData = dump_values(filename=config.dump_metadata_files(Streams)[1], flag='PICK')
    print(Loaded_Streamdata)
    print(formated_logstreamdata)
    check_for_new_logs(OldStreamData=Loaded_Streamdata, NewStreamData=formated_logstreamdata)
    print(Download_LogFiles)
    try:
        for Logfiles in Download_LogFiles:
            '''This statement will be executed if the log file is newly generated'''
            if Download_LogFiles[Logfiles] == 'NLF':
                '''creating a empty key in the dict for the new logfile'''
                Loaded_LogData[Logfiles] = {}
                Loaded_LogData[Logfiles]['nextToken'] = 0
                Tailflag = True
                '''This while loop will run until the end of the log file is downloaded'''
                while True:
                    Next_Token = Loaded_LogData[Logfiles]['nextToken']
                    raw_logs = aws_connect.get_log_events(LogGroupName=Logfiles, LogStreamName=Streams,
                                                          startFromHead=Tailflag, nextToken=Next_Token)
                    [log_to_file(config.dump_metadata_files(Streams)[2], msg) for msg in raw_logs['events']]
                    Loaded_LogData[Logfiles]['nextToken'] = raw_logs['nextToken']
                    '''Setting the Tailflag variable to False as first iteration would have downloaded the first 10000
                    lines of the file(Till 1 mb)'''
                    Tailflag = False
                    '''Terminate the while loop if the lasttoken and current token matches'''
                    if Loaded_LogData[Logfiles]['nextToken'] == raw_logs['nextToken']:
                        del raw_logs['events']
                        Dump_LogData[Logfiles] = raw_logs
                        make_logfile_for_kibana.append(Streams)
                        print('exiting loop')
                        break
            elif Download_LogFiles[Logfiles] == 'OLFA':
                while True:
                    Next_Token = Loaded_LogData[Logfiles]['nextToken']
                    raw_logs = aws_connect.get_log_events(LogGroupName=Logfiles, LogStreamName=Streams,
                                                          nextToken=Next_Token)
                    [log_to_file(config.dump_metadata_files(Streams)[2], msg) for msg in raw_logs['events']]
                    Loaded_LogData[Logfiles]['nextToken'] = raw_logs['nextToken']
                    if Loaded_LogData[Logfiles]['nextToken'] == raw_logs['nextToken']:
                        del raw_logs['events']
                        Dump_LogData[Logfiles] = raw_logs
                        make_logfile_for_kibana.append(Streams)
                        print('Exiting loop')
                        break
    except Exception as excp:
        print(excp)

for dowloadedfiles in make_logfile_for_kibana:
    cmd_grep = cmd_grep_raw.format(filename_in=config.dump_metadata_files(dowloadedfiles)[2], 
                                   filename_out=config.dump_metadata_files(dowloadedfiles)[3])
    cmd_output = execute_shell_commands(cmd_grep)[1]
    if cmd_output != None:
        print('Error while executing command')
        open(config.dump_metadata_files(dowloadedfiles)[2], 'w').close() 
