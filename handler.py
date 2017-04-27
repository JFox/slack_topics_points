# -*- coding: utf-8 -*-
__author__ = 'Heisenberg'
import json
import logging

from functools import partial
from operator import itemgetter
from random import SystemRandom
from urllib.parse import parse_qs

from data_access.aws import expected_token, topic_adapter


logger = logging.getLogger()
logger.setLevel(logging.INFO)

secure_random = SystemRandom()


def _get_action_result(action, text, params):
    result = ACTIONS[action](text=text, params=params)
    if isinstance(result, str):
        result = {'text': result}
    # result['parse'] = 'full'
    return json.dumps(result)


def _unlist_params(params):
    return {k: v[0] for k, v in params.items()}


def get_help(text, params):
    return """Hey there! I'm the points guy, these are some usage examples:

    `/points addTopic <topicName>` -> Add a new topic to track some points
    `/points give/remove @pepito.madueno points in <topic>` -> Give or remove a point to/from the given user
    `/points rank top/bottom <x> in <topic>` -> List the <x> top/bottom user for the given topic
    `/points list` -> List the existing topics
    `/points help` -> Display this helpful guide!"""


def add_topic(text, params):
    split = text.split(' ')
    if len(split) > 2:
        return 'The message format is wrong. Check it out, pls'

    topic = split[0]
    list_hidden = split[-1] == 'listHidden'
    if ' ' in topic:
        return 'Sorry, mate... Spaces are not allowed in the topic name.'

    created = topic_adapter.add_new_topic(name=topic, hidden=list_hidden, channels=[],
                                          allow_remove=True, choose_by='min')
    if created:
        return 'Success! The topic was created. Hit it! Hit it!'
    return 'That topic already exists, go hit it!'


def update_points(text, params, num_of_points):
    if num_of_points == 0:
        return 'Give or remove some points, zero won\'t do anything'

    split = text.split(' ')
    if len(split) != 4:
        return 'The message format is wrong. Check it out, pls'

    user = split[0]
    topic_name = split[-1]

    topic = topic_adapter.get_topic(name=topic_name,
                                    projected_fields=['allowedChannels', 'allowPointsRemove'])
    if topic is None:
        return 'Topic {0} doesn\'t exist. Create it with `/topics addTopic {0}` first'.format(topic_name)

    if topic['allowedChannels'] and params['channel_id'] not in topic['allowedChannels']:
        return 'That topic is not allowed to be used on this channel'

    if not topic['allowPointsRemove'] and num_of_points < 0:
        return 'That topic is not allowed for points removal'

    giver = '<@{}|{}>'.format(params['user_id'], params['user_name'])
    channel = '<#{}|{}>'.format(params['channel_id'], params['channel_name'])
    topic_adapter.update_topic_points(topic_name, to=user, giver=giver, channel=channel, points=num_of_points)
    return {
        'response_type': 'in_channel',
        'text': 'Success! The points have been awarded to {} in _{}_ topic'.format(user, topic_name),
    }


def rank_people(text, params):
    split = text.split(' ')
    if len(split) != 4:
        return 'The message format is wrong. Check it out, pls'

    limit = int(split[1])
    topic_name = split[-1]

    topic = topic_adapter.get_topic(name=topic_name, projected_fields=['totals'])
    if topic is None:
        return 'Topic {0} doesn\'t exist. Create it with `/topics addTopic {0}` first'.format(topic_name)

    results = sorted(topic['totals'].items(), key=itemgetter(1), reverse=split[0] == 'top')[:limit]
    header = 'The {} {} users for the _{}_ topic are:'.format(limit, split[0], topic_name)
    users_totals = '\n'.join(['{} has {} points'.format(k, v) for k, v in results])
    return {
        'response_type': 'in_channel',
        'text': '{}\n\n{}'.format(header, users_totals),
    }


def list_topics(text, params):
    topics = topic_adapter.list_topics()
    if not topics:
        return 'There are no topics yet. Create one with `/topics addTopic <topic>` first'

    return {
        'response_type': 'in_channel',
        'text': 'The current topics are:',
        'attachments': [
            {
                'text': '\n'.join([i['topic'] for i in topics])
            }
        ]
    }


def choose_next(text, params):
    split = text.split(' ')
    if len(split) != 2:
        return 'The message format is wrong. Check it out, pls'

    topic_name = split[-1]
    topic = topic_adapter.get_topic(name=topic_name, projected_fields=['totals', 'chooseBy'])
    if topic is None:
        return 'Topic {0} doesn\'t exist. Create it with `/topics addTopic {0}` first'.format(topic_name)

    topic_totals = topic['totals']
    if topic['chooseBy'] == 'min':
        look_val = min(topic_totals.values())
    else:
        look_val = max(topic_totals.values())

    filtered = ['{} ({} points)'.format(k, v) for k, v in topic_totals.items() if v == look_val]
    return {
        'response_type': 'in_channel',
        'text': 'The chosen for _{}_ topic is:'.format(topic_name),
        'attachments': [
            {
                'text': secure_random.choice(filtered)
            }
        ]
    }


ACTIONS = {
    'help': get_help,
    'give': partial(update_points, num_of_points=1),
    'remove': partial(update_points, num_of_points=-1),
    'addTopic': add_topic,
    'rank': rank_people,
    'list': list_topics,
    'chooseNext': choose_next,
}


def lambda_handler(event, context):
    logger.info('Event received')
    logger.debug('Event: {}'.format(event))
    params = _unlist_params(parse_qs(event['body'], keep_blank_values=True))
    logger.debug('params: {}'.format(params))
    logger.debug('expected_token: {}'.format(expected_token))
    if params['token'] != expected_token:
        logger.info('Wrong token')
        return {'statusCode': '400', 'body': 'Not Allowed'}

    text = params['text']
    if not text:
        return {'body': 'Gimme some action to execute, dude! `/points help` for some usage examples'}

    logger.debug('Command text: {}'.format(text))
    split = text.split(' ', 1)
    action = split[0]
    action_text = split[-1]
    logger.debug('action: {}'.format(action))
    logger.debug('action text: {}'.format(action_text))
    if action not in ACTIONS.keys():
        return {'body': 'The action selected is not allowed. Use `/topics help` for usage examples'}

    return {'statusCode': '200',
            'body': _get_action_result(action=action, text=action_text, params=params),
            'headers': {'Content-Type': 'application/json'}}
