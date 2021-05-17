from unittest import TestCase
from unittest.mock import patch, MagicMock

from flask import Flask

from utils.cassandra import CassandraCluster

class CassandraTestCase(TestCase):
    def setUp(self):
        self.app = Flask('test_app')

    def test_init(self):
        cluster = CassandraCluster()
        self.assertIsNone(cluster.app)

        cluster = CassandraCluster(app=self.app)
        self.assertEqual(cluster.app, self.app)

        self.app.config['CASSANDRA_NODES'] = 'cassandra.example.com'
        cluster.init_app(self.app)
        self.assertEqual(cluster.nodes, ['cassandra.example.com'])

        self.app.config['CASSANDRA_NODES'] = ['cassandra.example.org']
        cluster.init_app(self.app)
        self.assertEqual(cluster.nodes, ['cassandra.example.org'])

    @patch('utils.cassandra.Cluster')
    @patch('utils.cassandra.PlainTextAuthProvider')
    def test_connect(self, auth_class, cluster_class):
        cluster = CassandraCluster(app=self.app)
        session = cluster.connect()
        # Default parameters
        cluster_class.assert_called_once_with(['localhost'], port=9042)
        auth_class.assert_not_called()
        self.assertIsNone(session)

        cluster_class.reset_mock()
        self.app.config['CASSANDRA_NODES'] = 'cassandra.example.com'
        cluster = CassandraCluster(app=self.app)
        session = cluster.connect()
        # Default parameters
        cluster_class.assert_called_once_with(['cassandra.example.com'],
                                              port=9042)
        self.assertIsNone(session)
        auth_class.assert_not_called()

        cluster_class.reset_mock()
        self.app.config['CASSANDRA_USER'] = 'username'
        self.app.config['CASSANDRA_PASSWORD'] = 'password'
        self.app.config['CASSANDRA_PORT'] = 1234
        cluster = CassandraCluster(app=self.app)
        session = cluster.connect()
        cluster_class.assert_called_once_with(['cassandra.example.com'],
                                              port=1234)
        self.assertIsNone(session)
        auth_class.assert_called_once_with('username', 'password')

        old_cluster = cluster
        cluster.init_app(self.app)
        self.assertEqual(old_cluster, cluster)
        session = cluster.connect(keyspace='keyspace')
        # FIXME: Mock cluster.connect so it returns a different value for
        # the two calls.
        cluster.cluster.connect = MagicMock(
            side_effect=[MagicMock(), MagicMock()])
        # Connecting to the same keyspace should yield the same session
        session2 = cluster.connect(keyspace='keyspace')
        self.assertEqual(session, session2)

        session2 = cluster.connect(keyspace='new_keyspace')
        self.assertNotEqual(session, session2)

    @patch('utils.cassandra.Cluster')
    def test_session_property(self, cluster_class):
        try:
            del self.app.config['CASSANDRA_KEYSPACE']
        except KeyError:
            pass

        cluster = CassandraCluster(app=self.app)
        self.assertIsNone(cluster.session)

        self.app.config['CASSANDRA_KEYSPACE'] = 'test'
        cluster.init_app(self.app)
        self.assertIsNotNone(cluster.session)
