'''Please add the stream name here'''
CLOUDWATCH_GROUPNAME = ['']

'''If new stream is added in the CLOUDWATCH_STREAM, Repective files should be created in the opt/cloudwatch-scripts/
 Directory and .pkl files should be touched !! '''


def dump_metadata_files(filename):
    return {
        'StreamNameShouldMatch -- > CLOUDWATCH_GROUPNAME': ['/opt/cloudwatch-scripts/Stream_name/stream_metadata.pkl',
                                                              '/opt/cloudwatch-scripts/Stream_name/log_metadata.pkl',
                                                             '/opt/cloudwatch-scripts/Stream_name/raw_out.log',
                                                             '/opt/cloudwatch-scripts/Stream_name/streamname.log']
    }[filename]