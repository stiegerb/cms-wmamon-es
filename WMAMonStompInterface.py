#!/usr/bin/env python
"""WMAgent Monitoring notification producer for CERN AMQ
Based on GNI notification producer
https://gitlab.cern.ch/it-monitoring/gni-producer/raw/master/gniproducer/notification_producer.py

Note: gitlab.cern.ch link is down
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

    FIXME: Fixed 'on_send', but others are probably not correct as well
    """
    def on_connecting(self, host_and_port):
        print('on_connecting %s %s' % host_and_port)

    def on_error(self, headers, message):
        print('received an error %s' % (headers, message))

    def on_message(self, headers, body):
        print('on_message %s %s' % (headers, body))

    def on_heartbeat(self):
        print('on_heartbeat')

    def on_send(self, frame):
        print('on_send HEADERS: %s, BODY: %s' % (str(frame.headers), frame.body))

    def on_connected(self, headers, body):
        print('on_connected %s %s' % (headers, body))

    def on_disconnected(self):
        print('on_disconnected')

    def on_heartbeat_timeout(self):
        print('on_heartbeat_timeout')

    def on_before_message(self, headers, body):
        print('on_before_message %s %s' % (headers, body))

        return (headers, body)


class WMAMonStompInterface(object):
    """
    Class to generate and send notification to a given Stomp broker
    and a given topic.

    :param username: The username to connect to the broker.
    :param password: The password to connect to the broker.
    :param host_and_ports: The hosts and ports list of the brokers.
        Default: [('agileinf-mb.cern.ch', 61213)]
    """

    # Optional notifications headers values
    HEADERS_V2 = [] # ['m_toplevel_hostgroup',
                    # 'm_snow', 'm_email', 'destination']

    # Optional notifications body values
    METADATA_V2 = [] # 'hostgroup', 'environment', 'is_essential',
                     # 'asset_id', 'description', 'notification_type',
                     # 'producer_source', 'state', 'egroup_name', 'fe_name',
                     # 'se_name', 'email_target', 'troubleshooting',
                     # 'snow_assignment_level', 'snow_grouping', 'snow_instance',
                     # 'snow_display_value', 'snow_id']

    def __init__(self, username, password,
                 host_and_ports=[('agileinf-mb.cern.ch', 61213)]): # FIXME It this correct also for us? 
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
        """

        conn = stomp.Connection(host_and_ports=self._host_and_ports)
        conn.set_listener('StompyListener', StompyListener())
        conn.start()
        conn.connect(username=self._username, passcode=self._password, wait=True)

        # Send all the notifications together
        while len(self.notifications) > 0:
            try:
                notification = self.notifications.pop(0)
                body = json.dumps(notification.pop('body'))
                destination = notification.pop('destination')
                conn.send(body,
                          headers=notification,
                          destination=destination,
                          ack='auto')
            except Exception, msg:
                self._logger.error('Notification: %s not send, error: %s' %
                              (str(notification), str(msg)))

        if conn.is_connected():
            conn.stop()

    def generate_notification_v2(self,
                                 payload, metric_id, metric_name, entity,
                                 m_producer='WMAMonStompInterface',
                                 m_submitter_environment='qa',      # FIXME What to put here?
                                 m_submitter_hostgroup='hostgroup', # FIXME What to put here?
                                 m_submitter_host='somehost',       # FIXME What to put here
                                 dest='/topic/cms.jobmon.wmagent',
                                 **kwargs):
        """
        Generates a notification with the specified data and appends it to a 
        queue.

        :param m_producer: The notification producer.
        :param m_submitter_hostgroup: The hostgroup of the machine producing
            the notification.
        :param m_submitter_host: The host producing the notification.
        :param payload: Actual notification data.
        :param metric_id: Id representing the notification. # FIXME: Created by me or someone else? Can I take the one from couch?
        :param metric_name: Name of the metric representing the notification. # FIXME?
        :param entity: Entity producing the notification. # FIXME?
        :param  dest: Topic for the Stomp broker.
            Default: /topic/monitoring.notification.generic
        :param kwargs: Optional arguments expecting any optional field in the
            notification

        :return: the generated notification
        """

        notification = {}
        notification['destination'] = dest
        notification.update(self._generate_notification_header_v2(
                                m_producer,
                                m_submitter_environment,
                                m_submitter_hostgroup,
                                m_submitter_host,
                                **kwargs))

        notification['body'] = self._generate_notification_body_v2(
                                   payload,
                                   metric_id,
                                   metric_name,
                                   entity,
                                   **kwargs)

        self.notifications.append(notification)

        # Return the notification in order to use it if needed
        return notification

    def _generate_notification_header_v2(self, producer, submitter_environment,
                                         submitter_hostgroup, submitter_host,
                                         **kwargs):
        """
        Generates a notification header for version 2.

        :param: (See generate_notification_v2)

        :return: the generated header
        """

        # Add mandatory fields
        headers = {
                   'm_type': 'wmagent_info',
                   'm_version': '1.1',
                   'm_producer': producer,
                   'm_submitter_environment': submitter_environment,
                   'm_submitter_hostgroup': submitter_hostgroup,
                   'm_submitter_host': submitter_host
        }

        # Add optional fields, don't make check as the dashboard consumer will
        for optional_field in self.HEADERS_V2:
            if optional_field in kwargs:
                headers[optional_field] = kwargs[optional_field]

        return headers

    def _generate_notification_body_v2(self, payload, metric_id, metric_name,
                                       entity, **kwargs):
        """
        Generates a notification body for version 2.

        :param: (See generate_notification_v2)

        :return: the generated body
        """

        body = {
            'payload': payload,
            'metadata': {
                'timestamp': int(time.time()), # FIXME: Is this supposed to be the current time or the time of the doc creation?
                'metric_id': metric_id,
                'metric_name': metric_name,
                'entity': entity,
                'uuid': str(uuid.uuid1()),
            }
        }

        for optional_field in self.METADATA_V2:
            if optional_field in kwargs:
                body['metadata'][optional_field] = kwargs[optional_field]

        return body