#!/usr/bin/env python
"""WMAgent Monitoring notification producer for CERN AMQ
Based on GNI notification producer:
https://gitlab.cern.ch/monitoring/gni-producer/blob/master/gniproducer/notification_producer.py
"""
import json
import logging
import stomp
import time
import uuid


class StompyListener(object):
    """
    Auxiliar listener class to fetch all possible states in the Stomp
    connection.
    """
    def __init__(self):
        self.logr = logging.getLogger(__name__)

    def on_connecting(self, host_and_port):
        self.logr.info('on_connecting %s %s' % host_and_port)

    def on_error(self, headers, message):
        self.logr.info('received an error %s' % (headers, message))

    def on_message(self, headers, body):
        self.logr.info('on_message %s %s' % (headers, body))

    def on_heartbeat(self):
        self.logr.info('on_heartbeat')

    def on_send(self, frame):
        self.logr.info('on_send HEADERS: %s, BODY: %s ...' % (str(frame.headers), str(frame.body)[:160]))

    def on_connected(self, headers, body):
        self.logr.info('on_connected %s %s' % (headers, body))

    def on_disconnected(self):
        self.logr.info('on_disconnected')

    def on_heartbeat_timeout(self):
        self.logr.info('on_heartbeat_timeout')

    def on_before_message(self, headers, body):
        self.logr.info('on_before_message %s %s' % (headers, body))

        return (headers, body)


class WMAMonStompInterface(object):
    """
    Class to generate and send notification to a given Stomp broker
    and a given topic.

    :param username: The username to connect to the broker.
    :param password: The password to connect to the broker.
    :param host_and_ports: The hosts and ports list of the brokers.
        Default: [('agileinf-mb.cern.ch', 61213)]
        Testbed: [('dashb-test-mb.cern.ch', 61113)]
    """

    # Optional fields to be added in notification header
    MORE_HEADERS = []  

    # Optional fields to be added in notification metadata
    MORE_METADATA = [] 

    # Version number to be added in header
    _version = '0.1'

    def __init__(self, username, password,
                 host_and_ports=[('agileinf-mb.cern.ch', 61213)]):
        self._host_and_ports = host_and_ports
        self._username = username
        self._password = password
        self._logger = logging.getLogger(__name__)

        # Notifications list, will act as a queue
        self.notifications = []

    def produce(self):
        """
        Dequeue all the notifications on the list and sent them to the
        Stomp broker.

        :return: a list of successfully sent notification bodies
        """

        conn = stomp.Connection(host_and_ports=self._host_and_ports)
        conn.set_listener('StompyListener', StompyListener())
        conn.start()
        conn.connect(username=self._username, passcode=self._password, wait=True)

        successfully_sent = []
        # Send all the notifications together
        while len(self.notifications) > 0:
            try:
                notification = self.notifications.pop(0)
                body = notification.pop('body')
                destination = notification.pop('destination')
                conn.send(destination=destination,
                          headers=notification,
                          body=json.dumps(body),
                          ack='auto')
                self._logger.warning('Notification %s sent' % str(notification))
                successfully_sent.append(body)
            except Exception, msg:
                self._logger.error('Notification: %s not send, error: %s' %
                              (str(notification), str(msg)))

        if conn.is_connected():
            conn.disconnect()

        return successfully_sent

    def make_notification(self, payload, id_,
                          producer='WMAMonStompInterface',
                          dest='/topic/cms.jobmon.wmagent',
                          **kwargs):
        """
        Generates a notification with the specified data and appends it to a 
        queue.

        :param producer: The notification producer.
            Default: WMAMonStompInterface
        :param payload: Actual notification data.
        :param id_: Id representing the notification.
        :param  dest: Topic for the Stomp broker.
            Default: /topic/cms.jobmon.wmagent
        :param kwargs: Optional arguments expecting any optional field in the
            notification

        :return: the generated notification
        """

        notification = {}
        notification['destination'] = dest
        notification.update(self._make_header(producer, **kwargs))

        notification['body'] = self._make_body(payload, id_, **kwargs)

        self.notifications.append(notification)

        # Return the notification in order to use it if needed
        return notification

    def _make_header(self, producer, **kwargs):
        """
        Generates a notification header

        :param: (See make_notification)

        :return: the generated header
        """

        # Add mandatory fields
        headers = {
                   'type': 'cms_wmagent_info',
                   'version': self._version,
                   'producer': producer
        }

        # Add optional fields, don't make check as the dashboard consumer will
        for key in self.MORE_HEADERS:
            if key in kwargs:
                headers[key] = kwargs[key]

        return headers

    def _make_body(self, payload, id_, **kwargs):
        """
        Generates a notification body for version 2.

        :param: (See generate_notification)

        :return: the generated body
        """

        body = {
            'payload': payload,
            'metadata': {
                'timestamp': int(time.time()),
                'id': id_,
                'uuid': str(uuid.uuid1()),
            }
        }

        for key in self.MORE_METADATA:
            if key in kwargs:
                body['metadata'][key] = kwargs[key]

        return body
