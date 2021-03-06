# -*- encoding: utf-8 -*-
#
# Copyright © 2012 New Dream Network, LLC (DreamHost)
#
# Author: Doug Hellmann <doug.hellmann@dreamhost.com>
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
"""Test events statistics retrieval."""

import datetime
import testscenarios

from oslo.config import cfg

from . import base
from ceilometer import sample
from ceilometer.publisher import rpc
from ceilometer.tests import db as tests_db

load_tests = testscenarios.load_tests_apply_scenarios


class TestMaxProjectVolume(base.FunctionalTest,
                           tests_db.MixinTestsWithBackendScenarios):

    PATH = '/meters/volume.size/statistics'

    def setUp(self):
        super(TestMaxProjectVolume, self).setUp()

        self.counters = []
        for i in range(3):
            c = sample.Sample(
                'volume.size',
                'gauge',
                'GiB',
                5 + i,
                'user-id',
                'project1',
                'resource-id-%s' % i,
                timestamp=datetime.datetime(2012, 9, 25, 10 + i, 30 + i),
                resource_metadata={'display_name': 'test-volume',
                                   'tag': 'self.counter',
                                   },
                source='source1',
            )
            self.counters.append(c)
            msg = rpc.meter_message_from_counter(
                c,
                cfg.CONF.publisher_rpc.metering_secret,
            )
            self.conn.record_metering_data(msg)

    def test_no_time_bounds(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            }])
        self.assertEqual(data[0]['max'], 7)
        self.assertEqual(data[0]['count'], 3)

    def test_start_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 7)
        self.assertEqual(data[0]['count'], 2)

    def test_start_timestamp_after(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T12:34:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 5)
        self.assertEqual(data[0]['count'], 1)

    def test_end_timestamp_before(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T09:54:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_start_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:32:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 6)
        self.assertEqual(data[0]['count'], 1)


class TestMaxResourceVolume(base.FunctionalTest,
                            tests_db.MixinTestsWithBackendScenarios):

    PATH = '/meters/volume.size/statistics'

    def setUp(self):
        super(TestMaxResourceVolume, self).setUp()

        self.counters = []
        for i in range(3):
            c = sample.Sample(
                'volume.size',
                'gauge',
                'GiB',
                5 + i,
                'user-id',
                'project1',
                'resource-id',
                timestamp=datetime.datetime(2012, 9, 25, 10 + i, 30 + i),
                resource_metadata={'display_name': 'test-volume',
                                   'tag': 'self.counter',
                                   },
                source='source1',
            )
            self.counters.append(c)
            msg = rpc.meter_message_from_counter(
                c,
                cfg.CONF.publisher_rpc.metering_secret,
            )
            self.conn.record_metering_data(msg)

    def test_no_time_bounds(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            }])
        self.assertEqual(data[0]['max'], 7)
        self.assertEqual(data[0]['count'], 3)

    def test_no_time_bounds_with_period(self):
        data = self.get_json(self.PATH,
                             q=[{'field': 'resource_id',
                                 'value': 'resource-id'}],
                             period=3600)
        self.assertEqual(len(data), 3)
        self.assertEqual(set(x['duration_start'] for x in data),
                         set([u'2012-09-25T10:30:00',
                              u'2012-09-25T12:32:00',
                              u'2012-09-25T11:31:00']))
        self.assertEqual(data[0]['period'], 3600)
        self.assertEqual(set(x['period_start'] for x in data),
                         set([u'2012-09-25T10:30:00',
                              u'2012-09-25T11:30:00',
                              u'2012-09-25T12:30:00']))

    def test_period_with_negative_value(self):
        resp = self.get_json(self.PATH, expect_errors=True,
                             q=[{'field': 'resource_id',
                                 'value': 'resource-id'}],
                             period=-1)
        self.assertEquals(400, resp.status_code)

    def test_start_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 7)
        self.assertEqual(data[0]['count'], 2)

    def test_start_timestamp_after(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T12:34:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 5)
        self.assertEqual(data[0]['count'], 1)

    def test_end_timestamp_before(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T09:54:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_start_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:32:00',
                                            },
                                           ])
        self.assertEqual(data[0]['max'], 6)
        self.assertEqual(data[0]['count'], 1)


class TestSumProjectVolume(base.FunctionalTest,
                           tests_db.MixinTestsWithBackendScenarios):

    PATH = '/meters/volume.size/statistics'

    def setUp(self):
        super(TestSumProjectVolume, self).setUp()

        self.counters = []
        for i in range(3):
            c = sample.Sample(
                'volume.size',
                'gauge',
                'GiB',
                5 + i,
                'user-id',
                'project1',
                'resource-id-%s' % i,
                timestamp=datetime.datetime(2012, 9, 25, 10 + i, 30 + i),
                resource_metadata={'display_name': 'test-volume',
                                   'tag': 'self.counter',
                                   },
                source='source1',
            )
            self.counters.append(c)
            msg = rpc.meter_message_from_counter(
                c,
                cfg.CONF.publisher_rpc.metering_secret,
            )
            self.conn.record_metering_data(msg)

    def test_no_time_bounds(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            }])
        expected = 5 + 6 + 7
        self.assertEqual(data[0]['sum'], expected)
        self.assertEqual(data[0]['count'], 3)

    def test_start_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        expected = 6 + 7
        self.assertEqual(data[0]['sum'], expected)
        self.assertEqual(data[0]['count'], 2)

    def test_start_timestamp_after(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T12:34:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           ])
        self.assertEqual(data[0]['sum'], 5)
        self.assertEqual(data[0]['count'], 1)

    def test_end_timestamp_before(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T09:54:00',
                                            },
                                           ])
        self.assertEqual(data, [])

    def test_start_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'project_id',
                                            'value': 'project1',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:32:00',
                                            },
                                           ])
        self.assertEqual(data[0]['sum'], 6)
        self.assertEqual(data[0]['count'], 1)


class TestSumResourceVolume(base.FunctionalTest,
                            tests_db.MixinTestsWithBackendScenarios):

    PATH = '/meters/volume.size/statistics'

    def setUp(self):
        super(TestSumResourceVolume, self).setUp()

        self.counters = []
        for i in range(3):
            c = sample.Sample(
                'volume.size',
                'gauge',
                'GiB',
                5 + i,
                'user-id',
                'project1',
                'resource-id',
                timestamp=datetime.datetime(2012, 9, 25, 10 + i, 30 + i),
                resource_metadata={'display_name': 'test-volume',
                                   'tag': 'self.counter',
                                   },
                source='source1',
            )
            self.counters.append(c)
            msg = rpc.meter_message_from_counter(
                c,
                cfg.CONF.publisher_rpc.metering_secret,
            )
            self.conn.record_metering_data(msg)

    def test_no_time_bounds(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            }])
        self.assertEqual(data[0]['sum'], 5 + 6 + 7)
        self.assertEqual(data[0]['count'], 3)

    def test_no_time_bounds_with_period(self):
        data = self.get_json(self.PATH,
                             q=[{'field': 'resource_id',
                                 'value': 'resource-id'}],
                             period=1800)
        self.assertEqual(len(data), 3)
        self.assertEqual(set(x['duration_start'] for x in data),
                         set([u'2012-09-25T10:30:00',
                              u'2012-09-25T12:32:00',
                              u'2012-09-25T11:31:00']))
        self.assertEqual(data[0]['period'], 1800)
        self.assertEqual(set(x['period_start'] for x in data),
                         set([u'2012-09-25T10:30:00',
                              u'2012-09-25T11:30:00',
                              u'2012-09-25T12:30:00']))

    def test_start_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            }])
        self.assertEqual(data[0]['sum'], 6 + 7)
        self.assertEqual(data[0]['count'], 2)

    def test_start_timestamp_with_period(self):
        data = self.get_json(self.PATH,
                             q=[{'field': 'resource_id',
                                 'value': 'resource-id'},
                                {'field': 'timestamp',
                                 'op': 'ge',
                                 'value': '2012-09-25T10:15:00'}],
                             period=7200)
        self.assertEqual(len(data), 2)
        self.assertEqual(set(x['duration_start'] for x in data),
                         set([u'2012-09-25T10:30:00',
                              u'2012-09-25T12:32:00']))
        self.assertEqual(data[0]['period'], 7200)
        self.assertEqual(set(x['period_start'] for x in data),
                         set([u'2012-09-25T10:15:00',
                              u'2012-09-25T12:15:00']))

    def test_start_timestamp_after(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T12:34:00',
                                            }])
        self.assertEqual(data, [])

    def test_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T11:30:00',
                                            }])
        self.assertEqual(data[0]['sum'], 5)
        self.assertEqual(data[0]['count'], 1)

    def test_end_timestamp_before(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'le',
                                            'value': '2012-09-25T09:54:00',
                                            }])
        self.assertEqual(data, [])

    def test_start_end_timestamp(self):
        data = self.get_json(self.PATH, q=[{'field': 'resource_id',
                                            'value': 'resource-id',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'ge',
                                            'value': '2012-09-25T11:30:00',
                                            },
                                           {'field': 'timestamp',
                                            'op': 'lt',
                                            'value': '2012-09-25T11:32:00',
                                            }])
        self.assertEqual(data[0]['sum'], 6)
        self.assertEqual(data[0]['count'], 1)
