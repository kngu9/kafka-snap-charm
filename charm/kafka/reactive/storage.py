import os

from charms.layer.kafka import Kafka

from charmhelpers.core import hookenv, unitdata

from charms.reactive import remove_state, hook, set_flag

from charmhelpers.core.hookenv import log


def create_or_get_brokerid(log_dir):
    broker_path = os.path.join(log_dir, '.broker_id')
    storageids = hookenv.storage_list('logs')
    broker_id = storageids[0].split('/')[1]

    if os.path.exists(broker_path):
        with open(broker_path, 'r') as f:
            try:
                broker_id = int(f.read().replace('\n', ''))
            except ValueError:
                hookenv.log('{}'.format('invalid broker id format'))
                hookenv.status_set(
                    'blocked', 'unable to validate broker id format')
    else:
        os.makedirs(log_dir)

        with open(broker_path, 'w+') as f:
            f.write(broker_id)

    return broker_id


@hook('logs-storage-attached')
def storage_attach():
    storageids = hookenv.storage_list('logs')
    if not storageids:
        hookenv.status_set('blocked', 'cannot locate attached storage')
        return
    storageid = storageids[0]

    mount = hookenv.storage_get('location', storageid)
    if not mount:
        hookenv.status_set('blocked', 'cannot locate attached storage mount')
        return

    log_dir = os.path.join(mount, "logs")
    unitdata.kv().set('kafka.storage.log_dir', log_dir)
    hookenv.log('Kafka logs storage attached at {}'.format(log_dir))

    broker_id = create_or_get_brokerid(log_dir)
    unitdata.kv().set('kafka.broker_id', broker_id)

    # Stop Kafka; removing the kafka.started state will trigger
    # a reconfigure if/when it's ready
    remove_state('kafka.configured')
    set_flag('kafka.force-reconfigure')


@hook('logs-storage-detaching')
def storage_detaching():
    unitdata.kv().unset('kafka.storage.log_dir')
    unitdata.kv().unset('kafka.broker_id')

    Kafka().stop()

    log('log storage detatched, reconfiguring to use temporary storage')

    remove_state('kafka.configured')
    set_flag('kafka.force-reconfigure')

    remove_state('kafka.started')
    remove_state('kafka.storage.logs.attached')
