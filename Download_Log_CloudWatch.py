import boto3
import pickle
from shutil import copy
import config
from subprocess import PIPE, Popen
import ipdb

'''Assigning variable here that are used inside DEF's and loops'''
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
    global Data_old
    if flag == 'DUMP':
        with open(filename, 'wb') as dump_file:
            pickle.dump(payload, dump_file)
            dump_file.close()
    elif flag == 'PICK':
        with open(filename, 'rb') as fetch_file:
            Data_old = pickle.load(fetch_file)
            fetch_file.close()
    return Data_old if (flag == 'PICK') else None


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


def describe_group(group_name):
    """
    This function will describe the
    log stream to filter the new data.
    :param group_name: Name of the steam
    :return: Log steam meta data values i.e logfile name , last modified time.
    """
    global LogGroupData_Raw
    try:
        LogGroupData_Raw = aws_connect.describe_log_streams(logGroupName=group_name,
                                                            orderBy='LastEventTime', descending=True, limit=50)
    except Exception as excp:
        print(excp)
    return LogGroupData_Raw['logStreams']


def format_loggroupdata(payload):
    """
    This function will format the logstream data
    by adding the filename as a key to its metadata.
    :param payload: Current logstream metadata
    :return:
    """
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
    global Download_LogStreamFiles
    OldLogFiles = OldGroupData.keys()
    NewLogFIles = NewGroupData.keys()
    for newfiles in list(set(NewLogFIles) - set(OldLogFiles)):
        Download_LogStreamFiles[newfiles] = 'NLF'
    for logfiles in NewGroupData:
        try:
            if NewGroupData[logfiles]['lastIngestionTime'] != OldGroupData[logfiles]['lastIngestionTime']:
                Download_LogStreamFiles[logfiles] = 'OLFA'
        except Exception as excp:
            print(excp)
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


ipdb.set_trace()
'''Log download process starts here !!'''
for Groups in LogGroupName:
    Groups_described = describe_group(Groups)
    formated_loggroupdata = format_loggroupdata(payload=Groups_described)
    Loaded_LogGroupdata = dump_values(filename=config.dump_metadata_files(Groups)[0], flag='PICK')
    Loaded_LogStreamData = dump_values(filename=config.dump_metadata_files(Groups)[1], flag='PICK')
    print(Loaded_LogGroupdata)
    print(formated_loggroupdata)
    check_for_new_logs(OldGroupData=Loaded_LogGroupdata, NewGroupData=formated_loggroupdata)
    print(Download_LogStreamFiles)
    try:
        for LogStreamfiles in Download_LogStreamFiles:
            '''This statement will be executed if the log file is newly generated'''
            if Download_LogStreamFiles[LogStreamfiles] == 'NLF':
                '''creating a empty key in the dict for the new logfile'''
                Loaded_LogStreamData[LogStreamfiles] = {}
                Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = 0
                Tailflag = True
                '''This while loop will run until the end of the log file is downloaded'''
                while True:
                    Next_Token = Loaded_LogStreamData[LogStreamfiles]['nextForwardToken']
                    raw_logs = aws_connect.get_log_events(LogGroupName=LogStreamfiles, logStreamName=Groups,
                                                          startFromHead=Tailflag, nextToken=Next_Token)
                    if raw_logs['events']:
                        [log_to_file(config.dump_metadata_files(Groups)[2],
                                     str(msg['message']+'\n')) for msg in raw_logs['events']]
                    Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = raw_logs['nextForwardToken']
                    '''Setting the Tailflag variable to False as first iteration would have downloaded the first 10000
                    lines of the file(Till 1 mb)'''
                    Tailflag = False
                    '''Terminate the while loop if the lasttoken and current token matches'''
                    if Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] == raw_logs['nextForwardToken']:
                        del raw_logs['events']
                        del raw_logs['ResponseMetadata']
                        Dump_LogStreamData[LogStreamfiles] = raw_logs
                        make_logfile_for_kibana.append(Groups)
                        print('exiting loop')
                        break
            elif Download_LogStreamFiles[LogStreamfiles] == 'OLFA':
                while True:
                    Next_Token = Loaded_LogStreamData[LogStreamfiles]['nextForwardToken']
                    raw_logs = aws_connect.get_log_events(logGroupName=Groups, logStreamName=LogStreamfiles,
                                                          nextToken=Next_Token)
                    if raw_logs['events']:
                        [log_to_file(config.dump_metadata_files(Groups)[2],
                                     str(msg['message']+'\n')) for msg in raw_logs['events']]
                    Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] = raw_logs['nextForwardToken']
                    if Loaded_LogStreamData[LogStreamfiles]['nextForwardToken'] == raw_logs['nextForwardToken']:
                        del raw_logs['events']
                        del raw_logs['ResponseMetadata']
                        Dump_LogStreamData[LogStreamfiles] = raw_logs
                        make_logfile_for_kibana.append(Groups)
                        print('Exiting loop')
                        break
    except Exception as excp:
        print(excp)
exit()
for downloadedfiles in make_logfile_for_kibana:
    cmd_grep = cmd_grep_raw.format(filename_in=config.dump_metadata_files(downloadedfiles)[2],
                                   filename_out=config.dump_metadata_files(downloadedfiles)[3])
    cmd_output = execute_shell_commands(cmd_grep)[1]
    if cmd_output != 'None':
        print('Error while executing command')
        open(config.dump_metadata_files(dowloadedfiles)[2], 'w').close()
    dump_values(filename=config.dump_metadata_files(downloadedfiles)[0], flag='DUMP', payload=Dump_LogStreamData)
    dump_values(filename=config.dump_metadata_files(downloadedfiles)[1], flag='DUMP', payload=Dump_LogGroupdata)

