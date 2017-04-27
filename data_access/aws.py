import boto3
import logging
import os

from base64 import b64decode
from datetime import datetime

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


__all__ = ['expected_token', 'topic_adapter']


ENCRYPTED_EXPECTED_TOKEN = os.environ['kmsEncryptedToken']
expected_token = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_EXPECTED_TOKEN))['Plaintext'].decode()


class TopicAdapter(object):
    table = boto3.resource('dynamodb').Table(os.environ['tableName'])

    def get_topic(self, name, projected_fields):
        logger.info('Looking for {} topic'.format(name))
        logger.debug('Projected fields: {}'.format(projected_fields))
        response = self.table.get_item(Key={'topic': name},
                                       ProjectionExpression=', '.join(projected_fields))
        logger.debug('Topic resp: {}'.format(response))
        return response.get('Item')

    def add_new_topic(self, name, hidden, channels=None, allow_remove=True, choose_by='min'):
        try:
            self.table.put_item(
                Item={'topic': name, 'allowedChannels': [] if channels is None else channels,
                      'allowPointsRemove': allow_remove, 'listHidden': hidden, 'totals': {},
                      'chooseBy': choose_by},
                ConditionExpression="attribute_not_exists(topic)"
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                return False
            else:
                raise

    def list_topics(self):
        response = self.table.scan(
            ProjectionExpression='topic',
            FilterExpression=Attr('listHidden').eq(False)
        )
        return response['Items']

    def update_topic_points(self, topic_name, to, giver, channel, points):
        details = [{
            'timestamp': datetime.utcnow().isoformat(),
            'from': giver,
            'to': to,
            'channel': channel,
            'points': points,
        }]

        self.table.update_item(
            Key={'topic': topic_name},
            UpdateExpression='SET details = list_append(if_not_exists(details, :empty_list), :details) '
                             'ADD totals.#usr :points',
            ExpressionAttributeNames={'#usr': to},
            ExpressionAttributeValues={':empty_list': [], ':details': [details], ':points': points},
            ConditionExpression="attribute_exists(topic)"
        )


topic_adapter = TopicAdapter()
