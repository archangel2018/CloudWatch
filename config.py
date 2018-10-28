'''Please add the stream name here'''
CLOUDWATCH_STREAMNAME = ['/aws/rds/instance/prod-replica-enc/audit']

'''If new stream is added in the CLOUDWATCH_STREAM, Repective files should be created in the opt/cloudwatch-scripts/
 Directory and .pkl files should be touched !! '''


def dump_matadata_files(filename):
    return {
        'StreamNameShouldMatch -- > CLOUDWATCH_STREAMNAME': ['/opt/cloudwatch-scripts/Stream_name/stream_metadata.pkl',
                                                              '/opt/cloudwatch-scripts/Stream_name/log_metadata.pkl']
    }[filename]