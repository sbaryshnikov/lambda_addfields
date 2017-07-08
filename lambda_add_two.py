from __future__ import print_function

import base64
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Loading function')
print('Loading function')

field21 = '-1'
field22 = 'x.x.x.x'
delimiter = '|'

def lambda_handler(event, context):
    output = []
    succeeded_record_cnt = 0
    failed_record_cnt = 0

    for record in event['records']:
        print('recordId', record['recordId'])
        print('recorddataBefore', record['data'])
        payload = base64.b64decode(record['data'])
        print('recorddataAfter', payload)

        _arr = payload.split(delimiter)

        if len(_arr) == 22:
            succeeded_record_cnt += 1
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': record['data']
            }
        elif len(_arr) == 20 and _arr[16] == 'dev_channel':
            succeeded_record_cnt += 1
            del _arr[len(_arr)-1]
            _arr += [field21, field22, '\n']
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(delimiter.join(_arr))
            }
        else:
            print('Parsing failed')
            failed_record_cnt += 1
            output_record = {
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            }

        output.append(output_record)

    print('Processing completed.  Successful records {}, Failed records {}.'.format(succeeded_record_cnt, failed_record_cnt))
    return {'records': output}
