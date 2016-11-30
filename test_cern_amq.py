#! /usr/bin/env python
import sys
import json

from WMAMonStompInterface import WMAMonStompInterface

if __name__ == '__main__':
    # read a json file
    data = None
    with open(sys.argv[1], 'r') as ifile:
        data = json.load(ifile)

    doc = data['rows'][0]['value']

    # username and password from local files
    username = open('username', 'r').read().strip()
    password = open('password', 'r').read().strip()

    # set up the interface
    stomp_interface = WMAMonStompInterface(username=username,
                                           password=password,
                                           host_and_ports=[('agileinf-mb.cern.ch', 61213)])

    # generate a notification
    id_ = doc.pop("_id", None)
    note = stomp_interface.generate_notification_v2(payload=doc,
                                                    metric_id=id_,
                                                    metric_name="wmamon", # ?
                                                    entity="wmamon", # ?
                                                    m_producer='WMAMonStompInterface',
                                                    m_submitter_environment='wmamon', # ?
                                                    m_submitter_host='somehostname',
                                                    dest='/topic/cms.jobmon.wmagent')

    # send it
    stomp_interface.produce() # stomp.exception.ConnectFailedException

