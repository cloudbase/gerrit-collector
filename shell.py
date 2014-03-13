# Copyright 2014 Cloudbase Solutions Srl
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import paramiko
import pymongo
import sys

from oslo.config import cfg

from collector.openstack.common import log as logging

opts = [
    cfg.StrOpt('ssh_private_key_path', default=None,
               help='SSH key used to connect to Gerrit'),
    cfg.StrOpt('ssh_username', default=None,
               help='Gerrit SSH username'),
    cfg.StrOpt('ssh_hostname', default='review.openstack.org',
               help='Gerrit SSH host'),
    cfg.IntOpt('ssh_port', default=29418,
               help='Gerrit SSH port'),
    cfg.StrOpt('mongodb_hostname', default='127.0.0.1',
               help='MongoDB host'),
    cfg.IntOpt('mongodb_port', default=27017,
               help='MongoDB port'),
    cfg.StrOpt('mongodb_db', default='gerrit-collector',
               help='MongoDB db'),
    cfg.StrOpt('mongodb_collection', default='events',
               help='MongoDB events collection'),
]

CONF = cfg.CONF
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


def connect_gerrit():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    k = paramiko.RSAKey.from_private_key_file(CONF.ssh_private_key_path)
    LOG.info("Connecting to Gerrit")
    ssh.connect(CONF.ssh_hostname, username=CONF.ssh_username,
                port=CONF.ssh_port, pkey=k)
    return ssh


def get_gerrit_stream(ssh):
    LOG.info("Executing gerrit stream-events")
    stdin, stdout, stderr = ssh.exec_command("gerrit stream-events")
    stdin.channel.shutdown_write()
    return stdout


def get_mongodb_events_coll():
    LOG.info("Connecting to MongoDB")
    client = pymongo.MongoClient(CONF.mongodb_hostname, CONF.mongodb_port)
    db = client[CONF.mongodb_db]
    return db[CONF.mongodb_collection]


def get_gerrit_events(stream, event_action):
    LOG.info("Getting Gerrit events")
    while True:
        data = stream.readline()
        event_action(json.loads(data))
        LOG.debug("Gerrit event added")


def main():
    CONF(sys.argv[1:])
    logging.setup('gerrit-collector')

    ssh = connect_gerrit()
    gerrit_stream = get_gerrit_stream(ssh)

    events = get_mongodb_events_coll()
    get_gerrit_events(gerrit_stream, lambda event: events.insert(event))


if __name__ == "__main__":
    main()
