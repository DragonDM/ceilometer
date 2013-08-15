# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Rackspace Hosting.
#
# Author: Monsyne Dragon <mdragon@rackspace.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import fnmatch
import os

import jsonpath_rw
from oslo.config import cfg
import six
import yaml

from ceilometer.openstack.common import log
from ceilometer.openstack.common import timeutils

from ceilometer.storage import models

OPTS = [
    cfg.StrOpt('event_definitions_cfg_file',
               default="event_definitions.yaml",
               help="Configuration file for event definitions"
               ),
    cfg.BoolOpt('allow_dropping_of_notifications',
                default=False,
                help='Drop notifications if no event definition matches. '
                '(Otherwise, we convert them with just the default traits)'),

]

cfg.CONF.register_opts(OPTS)

LOG = log.getLogger(__name__)


class EventDefinitionException(Exception):
    def __init__(self, message, definition_cfg):
        self.msg = message
        self.definition_cfg = definition_cfg

    def __str__(self):
        return '%s %s: %s' % (self.__class__.__name__,
                              self.definition_cfg, self.msg)


class TraitDefinition(object):

    def __init__(self, name, trait_cfg):
        self.cfg = trait_cfg
        self.name = name

        type_name = trait_cfg.get('type', 'text')

        #if 'fields' not in traits_cfg and plugin is None:
        if 'fields' not in trait_cfg:
            raise EventDefinitionException(
                "Required field in trait definition not specified: 'fields'",
                self.cfg)

        fields = trait_cfg['fields']
        if not isinstance(fields, six.string_types):
            # if not a string, we assume a list (mdragon)
            if len(fields) == 1:
                fields = fields[0]
            else:
                fields = '|'.join('(%s)' % path for path in fields)
        try:
            self.fields = jsonpath_rw.parse(fields)
        except Exception as e:
            raise EventDefinitionException(
                "Parse error in JSONPath specification '%s' for %s: %s" % (
                fields, name, e), self.cfg)
        self.trait_type = getattr(models.Trait, '%s_TYPE' % type_name.upper(),
                                  None)
        if self.trait_type is None:
            raise EventDefinitionException(
                "Invalid trait type '%s' for trait %s" % (type_name, name),
                self.cfg)

    @staticmethod
    def _convert_value(trait_type, value):
        #perhaps this should be on the models.Trait class ? (mdragon)
        if trait_type is models.Trait.INT_TYPE:
            return int(value)
        if trait_type is models.Trait.FLOAT_TYPE:
            return float(value)
        if trait_type is models.Trait.DATETIME_TYPE:
            return timeutils.normalize_time(timeutils.parse_isotime(value))
        return str(value)

    def to_trait(self, notification_body):
        values = [match.value for match in self.fields.find(notification_body)
                  if match.value is not None]

        if not values:
            return None
        value = self._convert_value(self.trait_type, values[0])
        return models.Trait(self.name, self.trait_type, value)


class EventDefinition(object):

    DEFAULT_TRAITS = dict(
        message_id=dict(type='text', fields='message_id'),
        service=dict(type='text', fields='publisher_id'),
        request_id=dict(type='text', fields='_context_request_id'),
        tenant_id=dict(type='text', fields='_context_tenant'),
    )

    def __init__(self, definition_cfg):
        self._included_types = []
        self._excluded_types = []
        self.traits = dict()
        self.cfg = definition_cfg

        try:
            event_type = definition_cfg['event_type']
            traits = definition_cfg['traits']
        except KeyError as err:
            raise EventDefinitionException(
                "Required field %s not specified" % err.args[0], self.cfg)

        if isinstance(event_type, six.string_types):
            event_type = [event_type]

        for t in event_type:
            if t.startswith('!'):
                self._excluded_types.append(t[1:])
            else:
                self._included_types.append(t)

        if self._excluded_types and not self._included_types:
            self._included_types.append('*')

        for trait_name in self.DEFAULT_TRAITS:
            self.traits[trait_name] = TraitDefinition(
                trait_name,
                self.DEFAULT_TRAITS[trait_name])
        for trait_name in traits:
            self.traits[trait_name] = TraitDefinition(
                trait_name,
                traits[trait_name])

    def included_type(self, event_type):
        for t in self._included_types:
            if fnmatch.fnmatch(event_type, t):
                return True
        return False

    def excluded_type(self, event_type):
        for t in self._excluded_types:
            if fnmatch.fnmatch(event_type, t):
                return True
        return False

    def match_type(self, event_type):
        if (self.included_type(event_type)
                and not self.excluded_type(event_type)):
            return True
        return False

    @property
    def is_catchall(self):
        if '*' in self._included_types and not self._excluded_types:
            return True
        return False

    @staticmethod
    def _extract_when(body):
        """Extract the generated datetime from the notification.
        """
        # NOTE: I am keeping the logic the same as it was in the collector,
        # However, *ALL* notifications should have a 'timestamp' field, it's
        # part of the notification envelope spec. If this was put here because
        # some openstack project is generating notifications without a
        # timestamp, then that needs to be filed as a bug with the offending
        # project (mdragon)
        when = body.get('timestamp', body.get('_context_timestamp'))
        if when:
            return timeutils.normalize_time(timeutils.parse_isotime(when))

        return timeutils.utcnow()

    def to_event(self, notification_body):
        event_type = notification_body['event_type']
        when = self._extract_when(notification_body)

        traits = (self.traits[t].to_trait(notification_body)
                  for t in self.traits)
        # Only accept non-None value traits ...
        traits = [trait for trait in traits if trait is not None]
        event = models.Event(event_type, when, traits)
        return event


class NotificationEventsConverter(object):
    """Notification Event Converter

    The NotificationEventsConverter handles the conversion of Notifications
    from openstack systems into Ceilometer Events.

    The conversion is handled according to event definitions in a config file.

    The config is a list of event definitions. Order is significant, a
    notification will be processed according to the FIRST definition that
    matches it's event_type.
    Each definition is a dictionary with the following keys (all are required):
        event_type: this is a list of notification event_types this definition
                    will handle. These can be wildcarded with unix shell glob
                    (not regex!) wildcards.
                    An exclusion listing (starting with a '!') will exclude any
                    types listed from matching. If ONLY exclusions are listed,
                    the definition will match anything not matching the
                    exclusions.
                    This item can also be a string, which will be taken as
                    equivalent to 1 item list.

                    Examples:
                    *	['compute.instance.exists'] will only match
                            compute.intance.exists notifications
                    *   "compute.instance.exists"   Same as above.
                    *   ["image.create", "image.delete"]  will match
                         image.create and image.delete, but not anything else.
                    *   'compute.instance.*" will match
                        compute.instance.create.start but not image.upload
                    *   ['*.start','*.end', '!scheduler.*'] will match
                        compute.instance.create.start, and image.delete.end,
                        but NOT compute.instance.exists or
                        scheduler.run_instance.start
                    *   '!image.*' matches any notification except image
                        notifications.
                    *   ['*', '!image.*']  same as above.
        traits:  dictionary, The keys are trait names, the values are the trait
                 definitions
            Each trait definiton is a dictionary with the following keys:
                type (optional): The data type for this trait. (as a string)
                    Valid options are: 'text', 'int', 'float' and 'datetime'
                    defaults to 'text' if not specified.
                fields:  a path specification for the field(s) in the
                    notification you wish to extract. The paths can be
                    specified with a dot syntax (e.g. 'payload.host').
                    dictionary syntax (e.g. 'payload[host]') is also supported.
                    in either case, if the key for the field you are looking
                    for contains special charecters, like '.', it will need to
                    be quoted (with double or single quotes) like so:
                          "payload.image_meta.'org.openstack__1__architecture'"
                    The syntax used for the field specification is a variant
                    of JSONPath, and is fairly flexable.
                    (see: https://github.com/kennknowles/python-jsonpath-rw
                    for more info)  Specifications can be written to match
                    multiple possible fields, the value for the trait will
                    be derived from the first matching field that exists
                    and has a non-null (i.e. is not None) value.
                    This configuration value is nomally a string, for
                    conveniance, can be specified as a list of specifications,
                    which will be OR'ed together (a union query in jsonpath
                    terms)

                plugin:

    """

    def __init__(self, events_config, add_catchall=True):
        self.definitions = [
            EventDefinition(event_def)
            for event_def in events_config]
        if add_catchall and not any(d.is_catchall for d in self.definitions):
            event_def = dict(event_type='*', traits={})
            self.definitions.append(EventDefinition(event_def))

    def to_event(self, notification_body):
        event_type = notification_body['event_type']
        message_id = notification_body['message_id']
        edef = None
        for d in self.definitions:
            if d.match_type(event_type):
                edef = d
                break

        if edef is None:
            # If allow_dropping_of_notifications is False, this should
            # never happen. (mdragon)
            LOG.debug('Dropping Notification %s (uuid:%s)' %
                     (event_type, message_id))
            return None

        return edef.to_event(notification_body)


def setup_events():
    """Setup the event definitions from yaml config file."""
    config_file = cfg.CONF.event_definitions_cfg_file
    if not os.path.exists(config_file):
        config_file = cfg.CONF.find_file(config_file)

    if config_file is not None:
        LOG.debug("Event Definitions configuration file: %s", config_file)

        with open(config_file) as cf:
            config = cf.read()

        events_config = yaml.safe_load(config)
    else:
        LOG.debug("No Event Definitions configuration file found!"
                  " Using default config.")
        events_config = []

    LOG.info("Event Definitions: %s", events_config)

    allow_drop = cfg.CONF.allow_dropping_of_notifications
    return NotificationEventsConverter(events_config,
                                       add_catchall=not allow_drop)
