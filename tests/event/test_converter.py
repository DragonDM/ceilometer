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

import datetime

import jsonpath_rw
import mock
from oslo.config import cfg as oslo_cfg
import six

from ceilometer.event import converter
from ceilometer.openstack.common import timeutils
from ceilometer.storage import models
from ceilometer.tests import base


class ConverterBase(base.TestCase):
    def _create_test_notification(self, event_type, message_id, **kw):
        return dict(event_type=event_type,
                    message_id=message_id,
                    priority="INFO",
                    publisher_id="compute.host-1-2-3",
                    timestamp="2013-08-08 21:06:37.803826",
                    payload=kw,
                    )

    def assertIsValidEvent(self, event, notification):
        self.assertIsNot(
            None, event,
            "Notification Dropped when not expected to be dropped:"
            " %s" % str(notification))
        self.assertIsInstance(event, models.Event)

    def assertIsNotValidEvent(self, event, notification):
        self.assertIs(
            None, event,
            "Notification NOT Dropped when expected to be dropped:"
            " %s" % str(notification))

    def assertHasTrait(self, event, name, value=None, dtype=None):
        traits = [trait for trait in event.traits if trait.name == name]
        self.assertTrue(
            len(traits) > 0,
            "Trait %s not found in event %s" % (name, event))
        trait = traits[0]
        if value is not None:
            self.assertEquals(trait.value, value)
        if dtype is not None:
            self.assertEquals(trait.dtype, dtype)
            if dtype == models.Trait.INT_TYPE:
                self.assertIsInstance(trait.value, int)
            if dtype == models.Trait.FLOAT_TYPE:
                self.assertIsInstance(trait.value, float)
            if dtype == models.Trait.DATETIME_TYPE:
                self.assertIsInstance(trait.value, datetime.datetime)
            if dtype == models.Trait.TEXT_TYPE:
                self.assertIsInstance(trait.value, six.string_types)

    def assertDoesNotHaveTrait(self, event, name):
        traits = [trait for trait in event.traits if trait.name == name]
        self.assertEqual(
            len(traits), 0,
            "Extra Trait %s found in event %s" % (name, event))

    def assertHasDefaultTraits(self, event):
        text = models.Trait.TEXT_TYPE
        self.assertHasTrait(event, 'message_id', dtype=text)
        self.assertHasTrait(event, 'service', dtype=text)

    def _cmp_tree(self, this, other):
        if hasattr(this, 'right') and hasattr(other, 'right'):
            return (self._cmp_tree(this.right, other.right) and
                    self._cmp_tree(this.left, other.left))
        if not hasattr(this, 'right') and not hasattr(other, 'right'):
            return this == other
        return False

    def assertPathsEqual(self, path1, path2):
        self.assertTrue(self._cmp_tree(path1, path2),
                        'JSONPaths not equivalent %s %s' % (path1, path2))


class TestTraitDefinition(ConverterBase):

    def setUp(self):
        super(TestTraitDefinition, self).setUp()
        self.n1 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0001",
            instance_uuid="uuid-for-instance-0001",
            instance_id="id-for-instance-0001",
            instance_uuid2=None,
            instance_id2=None,
            host='host-1-2-3',
            image_meta=dict(
                        disk_gb='20',
                        thing='whatzit'),
            foobar=50)

    def test_to_trait(self):
        cfg = dict(type='text', fields='payload.instance_id')
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.name, 'test_trait')
        self.assertEquals(t.dtype, models.Trait.TEXT_TYPE)
        self.assertEquals(t.value, 'id-for-instance-0001')

        cfg = dict(type='int', fields='payload.image_meta.disk_gb')
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.name, 'test_trait')
        self.assertEquals(t.dtype, models.Trait.INT_TYPE)
        self.assertEquals(t.value, 20)

    def test_to_trait_multiple(self):
        cfg = dict(type='text', fields=['payload.instance_id',
                                        'payload.instance_uuid'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 'id-for-instance-0001')

        cfg = dict(type='text', fields=['payload.instance_uuid',
                                        'payload.instance_id'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 'uuid-for-instance-0001')

    def test_to_trait_multiple_different_nesting(self):
        cfg = dict(type='int', fields=['payload.foobar',
                   'payload.image_meta.disk_gb'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 50)

        cfg = dict(type='int', fields=['payload.image_meta.disk_gb',
                   'payload.foobar'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 20)

    def test_to_trait_some_null_multiple(self):
        cfg = dict(type='text', fields=['payload.instance_id2',
                                        'payload.instance_uuid'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 'uuid-for-instance-0001')

    def test_to_trait_some_missing_multiple(self):
        cfg = dict(type='text', fields=['payload.not_here_boss',
                                        'payload.instance_uuid'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIsInstance(t, models.Trait)
        self.assertEquals(t.value, 'uuid-for-instance-0001')

    def test_to_trait_missing(self):
        cfg = dict(type='text', fields='payload.not_here_boss')
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_to_trait_null(self):
        cfg = dict(type='text', fields='payload.instance_id2')
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_to_trait_multiple_null_missing(self):
        cfg = dict(type='text', fields=['payload.not_here_boss',
                                        'payload.instance_id2'])
        tdef = converter.TraitDefinition('test_trait', cfg)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_missing_fields_config(self):
        self.assertRaises(converter.EventDefinitionException,
                          converter.TraitDefinition,
                          'bogus_trait',
                          dict())

    def test_string_fields_config(self):
        cfg = dict(fields='payload.test')
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertPathsEqual(t.fields, jsonpath_rw.parse('payload.test'))

    def test_list_fields_config(self):
        cfg = dict(fields=['payload.test', 'payload.other'])
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertPathsEqual(
            t.fields,
            jsonpath_rw.parse('(payload.test)|(payload.other)'))

    def test_invalid_path_config(self):
        #test invalid jsonpath...
        cfg = dict(fields='payload.bogus(')
        self.assertRaises(converter.EventDefinitionException,
                          converter.TraitDefinition,
                          'bogus_trait',
                          cfg)

    def test_type_config(self):
        cfg = dict(type='text', fields='payload.test')
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertEquals(t.trait_type, models.Trait.TEXT_TYPE)

        cfg = dict(type='int', fields='payload.test')
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertEquals(t.trait_type, models.Trait.INT_TYPE)

        cfg = dict(type='float', fields='payload.test')
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertEquals(t.trait_type, models.Trait.FLOAT_TYPE)

        cfg = dict(type='datetime', fields='payload.test')
        t = converter.TraitDefinition('test_trait', cfg)
        self.assertEquals(t.trait_type, models.Trait.DATETIME_TYPE)

    def test_invalid_type_config(self):
        #test invalid jsonpath...
        cfg = dict(type='bogus', fields='payload.test')
        self.assertRaises(converter.EventDefinitionException,
                          converter.TraitDefinition,
                          'bogus_trait',
                          cfg)

    def test_convert_value(self):
        v = converter.TraitDefinition._convert_value(
            models.Trait.INT_TYPE, '10')
        self.assertEquals(v, 10)
        self.assertIsInstance(v, int)
        v = converter.TraitDefinition._convert_value(
            models.Trait.FLOAT_TYPE, '10')
        self.assertEquals(v, 10.0)
        self.assertIsInstance(v, float)

        v = converter.TraitDefinition._convert_value(
            models.Trait.DATETIME_TYPE, '2013-08-08 21:05:37.123456')
        self.assertEquals(v, datetime.datetime(2013, 8, 8, 21, 5, 37, 123456))
        self.assertIsInstance(v, datetime.datetime)

        v = converter.TraitDefinition._convert_value(
            models.Trait.TEXT_TYPE, 10)
        self.assertEquals(v, "10")
        self.assertIsInstance(v, str)


class TestEventDefinition(ConverterBase):

    def setUp(self):
        super(TestEventDefinition, self).setUp()

        self.traits_cfg = {
            'instance_id': {
                'type': 'text',
                'fields': ['payload.instance_uuid',
                           'payload.instance_id'],
            },
            'host': {
                'type': 'text',
                'fields': 'payload.host',
            },
        }

        self.test_notification1 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0001",
            instance_id="uuid-for-instance-0001",
            host='host-1-2-3')

        self.test_notification2 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0002",
            instance_id="uuid-for-instance-0002")

        self.test_notification3 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0003",
            instance_id="uuid-for-instance-0003",
            host=None)

    def test_to_event(self):
        dtype = models.Trait.TEXT_TYPE
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)

        e = edef.to_event(self.test_notification1)
        self.assertEquals(e.event_name, 'test.thing')
        self.assertEquals(e.generated,
                          datetime.datetime(2013, 8, 8, 21, 6, 37, 803826))

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'host', value='host-1-2-3', dtype=dtype)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0001',
                            dtype=dtype)

    def test_to_event_missing_trait(self):
        dtype = models.Trait.TEXT_TYPE
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)

        e = edef.to_event(self.test_notification2)

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0002',
                            dtype=dtype)
        self.assertDoesNotHaveTrait(e, 'host')

    def test_to_event_null_trait(self):
        dtype = models.Trait.TEXT_TYPE
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)

        e = edef.to_event(self.test_notification3)

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0003',
                            dtype=dtype)
        self.assertDoesNotHaveTrait(e, 'host')

    def test_bogus_cfg_no_traits(self):
        bogus = dict(event_type='test.foo')
        self.assertRaises(converter.EventDefinitionException,
                          converter.EventDefinition,
                          bogus)

    def test_bogus_cfg_no_type(self):
        bogus = dict(traits=self.traits_cfg)
        self.assertRaises(converter.EventDefinitionException,
                          converter.EventDefinition,
                          bogus)

    def test_included_type_string(self):
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertEquals(len(edef._included_types), 1)
        self.assertEquals(edef._included_types[0], 'test.thing')
        self.assertEquals(len(edef._excluded_types), 0)
        self.assertTrue(edef.included_type('test.thing'))
        self.assertFalse(edef.excluded_type('test.thing'))
        self.assertTrue(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('random.thing'))

    def test_included_type_list(self):
        cfg = dict(event_type=['test.thing', 'other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertEquals(len(edef._included_types), 2)
        self.assertEquals(len(edef._excluded_types), 0)
        self.assertTrue(edef.included_type('test.thing'))
        self.assertTrue(edef.included_type('other.thing'))
        self.assertFalse(edef.excluded_type('test.thing'))
        self.assertTrue(edef.match_type('test.thing'))
        self.assertTrue(edef.match_type('other.thing'))
        self.assertFalse(edef.match_type('random.thing'))

    def test_excluded_type_string(self):
        cfg = dict(event_type='!test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertEquals(len(edef._included_types), 1)
        self.assertEquals(edef._included_types[0], '*')
        self.assertEquals(edef._excluded_types[0], 'test.thing')
        self.assertEquals(len(edef._excluded_types), 1)
        self.assertEquals(edef._excluded_types[0], 'test.thing')
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.included_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_excluded_type_list(self):
        cfg = dict(event_type=['!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertEquals(len(edef._included_types), 1)
        self.assertEquals(len(edef._excluded_types), 2)
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.excluded_type('other.thing'))
        self.assertFalse(edef.excluded_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('other.thing'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_mixed_type_list(self):
        cfg = dict(event_type=['*.thing', '!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertEquals(len(edef._included_types), 1)
        self.assertEquals(len(edef._excluded_types), 2)
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.excluded_type('other.thing'))
        self.assertFalse(edef.excluded_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('other.thing'))
        self.assertFalse(edef.match_type('random.whatzit'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_catchall(self):
        cfg = dict(event_type=['*.thing', '!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['!other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['*', '!other.thing'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['*'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertTrue(edef.is_catchall)

        cfg = dict(event_type=['*', 'foo'],
                   traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        self.assertTrue(edef.is_catchall)

    def test_extract_when(self):
        now = timeutils.utcnow()
        modified = now + datetime.timedelta(minutes=1)
        timeutils.set_time_override(now)

        body = {"timestamp": str(modified)}
        self.assertEquals(converter.EventDefinition._extract_when(body),
                          modified)

        body = {"_context_timestamp": str(modified)}
        self.assertEquals(converter.EventDefinition._extract_when(body),
                          modified)

        then = now + datetime.timedelta(hours=1)
        body = {"timestamp": str(modified), "_context_timestamp": str(then)}
        self.assertEquals(converter.EventDefinition._extract_when(body),
                          modified)

        self.assertEquals(converter.EventDefinition._extract_when({}), now)

    def test_default_traits(self):
        cfg = dict(event_type='test.thing', traits={})
        edef = converter.EventDefinition(cfg)
        default_traits = converter.EventDefinition.DEFAULT_TRAITS.keys()
        traits = set(edef.traits.keys())
        for dt in default_traits:
            self.assertIn(dt, traits)
        self.assertEquals(len(edef.traits),
                          len(converter.EventDefinition.DEFAULT_TRAITS))

    def test_traits(self):
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = converter.EventDefinition(cfg)
        default_traits = converter.EventDefinition.DEFAULT_TRAITS.keys()
        traits = set(edef.traits.keys())
        for dt in default_traits:
            self.assertIn(dt, traits)
        self.assertIn('host', traits)
        self.assertIn('instance_id', traits)
        self.assertEquals(len(edef.traits),
                          len(converter.EventDefinition.DEFAULT_TRAITS) + 2)


class TestNotificationConverter(ConverterBase):

    def setUp(self):
        super(TestNotificationConverter, self).setUp()

        self.valid_event_def1 = [{
            'event_type': 'compute.instance.create.*',
            'traits': {
                'instance_id': {
                    'type': 'text',
                    'fields': ['payload.instance_uuid',
                               'payload.instance_id'],
                },
                'host': {
                    'type': 'text',
                    'fields': 'payload.host',
                },
            },
        }]

        self.test_notification1 = self._create_test_notification(
            "compute.instance.create.start",
            "uuid-for-notif-0001",
            instance_id="uuid-for-instance-0001",
            host='host-1-2-3')
        self.test_notification2 = self._create_test_notification(
            "bogus.notification.from.mars",
            "uuid-for-notif-0002",
            weird='true',
            host='cydonia')

    def test_converter_with_catchall(self):
        c = converter.NotificationEventsConverter(
            self.valid_event_def1,
            add_catchall=True)
        self.assertEquals(len(c.definitions), 2)
        e = c.to_event(self.test_notification1)
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEquals(len(e.traits), 4)
        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id')
        self.assertHasTrait(e, 'host')

        e = c.to_event(self.test_notification2)
        self.assertIsValidEvent(e, self.test_notification2)
        self.assertEquals(len(e.traits), 2)
        self.assertHasDefaultTraits(e)
        self.assertDoesNotHaveTrait(e, 'instance_id')
        self.assertDoesNotHaveTrait(e, 'host')

    def test_converter_without_catchall(self):
        c = converter.NotificationEventsConverter(
            self.valid_event_def1,
            add_catchall=False)
        self.assertEquals(len(c.definitions), 1)
        e = c.to_event(self.test_notification1)
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEquals(len(e.traits), 4)
        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id')
        self.assertHasTrait(e, 'host')

        e = c.to_event(self.test_notification2)
        self.assertIsNotValidEvent(e, self.test_notification2)

    def test_converter_empty_cfg_with_catchall(self):
        c = converter.NotificationEventsConverter(
            [],
            add_catchall=True)
        self.assertEquals(len(c.definitions), 1)
        e = c.to_event(self.test_notification1)
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEquals(len(e.traits), 2)
        self.assertHasDefaultTraits(e)

        e = c.to_event(self.test_notification2)
        self.assertIsValidEvent(e, self.test_notification2)
        self.assertEquals(len(e.traits), 2)
        self.assertHasDefaultTraits(e)

    def test_converter_empty_cfg_without_catchall(self):
        c = converter.NotificationEventsConverter(
            [],
            add_catchall=False)
        self.assertEquals(len(c.definitions), 0)
        e = c.to_event(self.test_notification1)
        self.assertIsNotValidEvent(e, self.test_notification1)

        e = c.to_event(self.test_notification2)
        self.assertIsNotValidEvent(e, self.test_notification2)

    def test_setup_events_default_config(self):

        def mock_exists(path):
            return False

        oslo_cfg.CONF.set_override('allow_dropping_of_notifications', False)

        with mock.patch('os.path.exists', mock_exists):
            c = converter.setup_events()
        self.assertIsInstance(c, converter.NotificationEventsConverter)
        self.assertEquals(len(c.definitions), 1)
        self.assertTrue(c.definitions[0].is_catchall)

        oslo_cfg.CONF.set_override('allow_dropping_of_notifications', True)

        with mock.patch('os.path.exists', mock_exists):
            c = converter.setup_events()
        self.assertIsInstance(c, converter.NotificationEventsConverter)
        self.assertEquals(len(c.definitions), 0)
