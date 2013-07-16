# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack LLC.
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

"""Volume drivers for libvirt."""

import os
import time

from nova import exception
from nova import flags
from nova.openstack.common import log as logging
from nova import utils
from nova.virt.libvirt import config
from nova.virt.libvirt import utils as virtutils
from nova.api.ec2 import ec2utils

LOG = logging.getLogger(__name__)
FLAGS = flags.FLAGS
flags.DECLARE('num_iscsi_scan_tries', 'nova.volume.driver')


class LibvirtVolumeDriver(object):
    """Base class for volume drivers."""
    def __init__(self, connection):
        self.connection = connection

    def connect_volume(self, connection_info, mount_device):
        """Connect the volume. Returns xml for libvirt."""
        conf = config.LibvirtConfigGuestDisk()
        conf.source_type = "block"
        conf.driver_name = virtutils.pick_disk_driver_name(is_block_dev=True)
        conf.driver_format = "raw"
        conf.driver_cache = "none"
        conf.source_path = connection_info['data']['device_path']
        conf.target_dev = mount_device
        conf.target_bus = "virtio"
        conf.serial = connection_info.get('serial')

        volume_id = connection_info['data']['volume_id']

        # TODO: ec2_volume_id have to be computed from FLAGS.volume_name_template
        local_volume_ec2id = "/dev/%s/%s" % (FLAGS.volume_group, ec2utils.id_to_ec2_vol_id(volume_id))

        local_volume_uuid = "/dev/%s/%s" % (FLAGS.volume_group, FLAGS.volume_name_template)
        local_volume_uuid = local_volume_uuid % volume_id
        is_local_volume_ec2id = os.path.islink(local_volume_ec2id)
        is_local_volume_uuid = os.path.islink(local_volume_uuid)

        if is_local_volume_uuid:
             conf.source_path = local_volume_uuid
        elif is_local_volume_ec2id:
             conf.source_path = local_volume_ec2id
        else:
             LOG.debug("Attaching device %s as %s" % (conf.source_path, mount_device))
        return conf

    def disconnect_volume(self, connection_info, mount_device):
        """Disconnect the volume"""
        pass


class LibvirtFakeVolumeDriver(LibvirtVolumeDriver):
    """Driver to attach Network volumes to libvirt."""

    def connect_volume(self, connection_info, mount_device):
        conf = config.LibvirtConfigGuestDisk()
        conf.source_type = "network"
        conf.driver_name = "qemu"
        conf.driver_format = "raw"
        conf.driver_cache = "none"
        conf.source_protocol = "fake"
        conf.source_host = "fake"
        conf.target_dev = mount_device
        conf.target_bus = "virtio"
        conf.serial = connection_info.get('serial')
        return conf


class LibvirtNetVolumeDriver(LibvirtVolumeDriver):
    """Driver to attach Network volumes to libvirt."""

    def connect_volume(self, connection_info, mount_device):
        conf = config.LibvirtConfigGuestDisk()
        conf.source_type = "network"
        conf.driver_name = virtutils.pick_disk_driver_name(is_block_dev=False)
        conf.driver_format = "raw"
        conf.driver_cache = "none"
        conf.source_protocol = connection_info['driver_volume_type']
        conf.source_host = connection_info['data']['name']
        conf.target_dev = mount_device
        conf.target_bus = "virtio"
        conf.serial = connection_info.get('serial')
        netdisk_properties = connection_info['data']
        if netdisk_properties.get('auth_enabled'):
            conf.auth_username = netdisk_properties['auth_username']
            conf.auth_secret_type = netdisk_properties['secret_type']
            conf.auth_secret_uuid = netdisk_properties['secret_uuid']
        return conf


class LibvirtISCSIVolumeDriver(LibvirtVolumeDriver):
    """Driver to attach Network volumes to libvirt."""

    def _run_iscsiadm(self, iscsi_properties, iscsi_command, **kwargs):
        check_exit_code = kwargs.pop('check_exit_code', 0)
        (out, err) = utils.execute('iscsiadm', '-m', 'node', '-T',
                                   iscsi_properties['target_iqn'],
                                   '-p', iscsi_properties['target_portal'],
                                   *iscsi_command, run_as_root=True,
                                   check_exit_code=check_exit_code)
        LOG.debug("iscsiadm %s: stdout=%s stderr=%s" %
                  (iscsi_command, out, err))
        return (out, err)

    def _iscsiadm_update(self, iscsi_properties, property_key, property_value,
                         **kwargs):
        iscsi_command = ('--op', 'update', '-n', property_key,
                         '-v', property_value)
        return self._run_iscsiadm(iscsi_properties, iscsi_command, **kwargs)

    @utils.synchronized('connect_volume')
    def connect_volume(self, connection_info, mount_device):
        """Attach the volume to instance_name"""
        iscsi_properties = connection_info['data']
        # NOTE(vish): If we are on the same host as nova volume, the
        #             discovery makes the target so we don't need to
        #             run --op new. Therefore, we check to see if the
        #             target exists, and if we get 255 (Not Found), then
        #             we run --op new. This will also happen if another
        #             volume is using the same target.
        try:
            self._run_iscsiadm(iscsi_properties, ())
        except exception.ProcessExecutionError as exc:
            # iscsiadm returns 21 for "No records found" after version 2.0-871
            if exc.exit_code in [21, 255]:
                self._run_iscsiadm(iscsi_properties, ('--op', 'new'))
            else:
                raise

        if iscsi_properties.get('auth_method'):
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.authmethod",
                                  iscsi_properties['auth_method'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.username",
                                  iscsi_properties['auth_username'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.password",
                                  iscsi_properties['auth_password'])

        # NOTE(vish): If we have another lun on the same target, we may
        #             have a duplicate login
        self._run_iscsiadm(iscsi_properties, ("--login",),
                           check_exit_code=[0, 255])

        self._iscsiadm_update(iscsi_properties, "node.startup", "automatic")

        host_device = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s" %
                        (iscsi_properties['target_portal'],
                         iscsi_properties['target_iqn'],
                         iscsi_properties.get('target_lun', 0)))

	volume_id = connection_info['data']['volume_id']
	device_ec2id = "/dev/%s/%s" % (FLAGS.volume_group, ec2utils.id_to_ec2_vol_id(volume_id))
	
        # we've just found that old EC2 id based device of volume exist, that's very likely a 
        # migrated volume and it is worth to use it instead of new uuid based device which 
        # could not exist at all
        if os.path.exists(device_ec2id):
	    target_iqn = 'iqn.2010-10.org.openstack:%s' % FLAGS.volume_name_template
	    target_iqn = target_iqn % device_ec2id
	    host_device = device_ec2id
	    iscsi_properties['target_iqn']=target_iqn
	    
	    msg = 'Found EC2 volume ID: %s, attaching pre-migration volume' % (device_ec2id)
	    LOG.debug(msg)

        # The /dev/disk/by-path/... node is not always present immediately
        # TODO(justinsb): This retry-with-delay is a pattern, move to utils?
        tries = 0
        while not os.path.exists(host_device):
            if tries >= FLAGS.num_iscsi_scan_tries:
                raise exception.NovaException(_("iSCSI device not found at %s")
                                              % (host_device))

            LOG.warn(_("ISCSI volume not yet found at: %(mount_device)s. "
                       "Will rescan & retry.  Try number: %(tries)s") %
                     locals())

            # The rescan isn't documented as being necessary(?), but it helps
            self._run_iscsiadm(iscsi_properties, ("--rescan",))

            tries = tries + 1
            if not os.path.exists(host_device):
                time.sleep(tries ** 2)

        if tries != 0:
            LOG.debug(_("Found iSCSI node %(mount_device)s "
                        "(after %(tries)s rescans)") %
                      locals())

        connection_info['data']['device_path'] = host_device
        sup = super(LibvirtISCSIVolumeDriver, self)
        return sup.connect_volume(connection_info, mount_device)

    @utils.synchronized('connect_volume')
    def disconnect_volume(self, connection_info, mount_device):
        """Detach the volume from instance_name"""
        sup = super(LibvirtISCSIVolumeDriver, self)
        sup.disconnect_volume(connection_info, mount_device)
        iscsi_properties = connection_info['data']
        # NOTE(vish): Only disconnect from the target if no luns from the
        #             target are in use.
        device_prefix = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-" %
                         (iscsi_properties['target_portal'],
                          iscsi_properties['target_iqn']))
        devices = self.connection.get_all_block_devices()
        devices = [dev for dev in devices if dev.startswith(device_prefix)]
        if not devices:
            self._iscsiadm_update(iscsi_properties, "node.startup", "manual",
                                  check_exit_code=[0, 21, 255])
            self._run_iscsiadm(iscsi_properties, ("--logout",),
                               check_exit_code=[0, 21, 255])
            self._run_iscsiadm(iscsi_properties, ('--op', 'delete'),
                               check_exit_code=[0, 21, 255])
        else:
            # can't close the session, at least delete the device for disconnected vol
            if not 'target_lun' in iscsi_properties:
                return
            host_device = device_prefix+str(iscsi_properties['target_lun'])
            device_shortname = os.path.basename(os.path.realpath(host_device))
            delete_ctl_file = '/sys/block/'+device_shortname+'/device/delete'
            if os.path.exists(delete_ctl_file):
                # echo 1 > /sys/block/sdX/device/delete
                utils.execute('cp', '/dev/stdin', delete_ctl_file,
                              process_input='1', run_as_root=True)
