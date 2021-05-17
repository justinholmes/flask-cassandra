# -*- coding: utf-8 -*-
'''
flask-cassandra

Flask-Cassandra provides an application-level connection to an Apache
Cassandra database. This connection can be used to interact with a Cassandra
cluster.

Updated by Gergely Polonkai at GT2.

:copyright: (c) 2015 by Terbium Labs.
:license: BSD, see LICENSE for more details.
'''

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack

__version_info__ = ('0', '1', '4')
__version__ = '.'.join(__version_info__)
__author__ = 'Michael Moore'
__license__ = 'BSD'
__copyright__ = '(c) 2015 by TerbiumLabs'

try:
    string_types = (str, unicode)
except NameError:
    # Python 3 doesn’t have unicode, only str
    string_types = (str,)


class CassandraCluster(object):
    """
    Cassandra cluster access for Flask apps.
    """

    def __init__(self, app=None):
        self.app = app
        self.cluster = None
        self.__primary_session = None
        self.__session = {}
        self.__queries = {}

        self.nodes = []
        self.port = 9042
        self.user = None
        self.password = None
        self.keyspace = None
        self.default_consistency_level = None

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        # Set some reasonable defaults for config values
        app.config.setdefault('CASSANDRA_CONSISTENCY_LEVEL', None)
        app.config.setdefault('CASSANDRA_NODES', ['localhost'])
        app.config.setdefault('CASSANDRA_PORT', 9042)
        app.config.setdefault('CASSANDRA_USER', None)
        app.config.setdefault('CASSANDRA_PASSWORD', None)
        app.config.setdefault('CASSANDRA_KEYSPACE', None)

        # Let’s be on the safe side, and convert CASSANDRA_NODES to a list
        # if it’s a string
        if isinstance(app.config['CASSANDRA_NODES'], string_types):
            app.config['CASSANDRA_NODES'] = [app.config['CASSANDRA_NODES']]

        self.app = app

        self.nodes = app.config['CASSANDRA_NODES']
        self.port = app.config['CASSANDRA_PORT']
        self.user = app.config['CASSANDRA_USER']
        self.password = app.config['CASSANDRA_PASSWORD']
        self.keyspace = app.config['CASSANDRA_KEYSPACE']
        self.default_consistency_level = app.config['CASSANDRA_CONSISTENCY_LEVEL']

        # It is possible we are being reinitialised with different
        # parameters, so let’s reset sessions
        self.__sessions = {}
        self.__queries = {}
        self.cluster = None

        # If there is an old CassandraCluster, shut it down; we will
        # probably create a new one
        ctx = stack.top

        if hasattr(ctx, 'cassandra_cluster'):
            ctx.cassandra_cluster.shutdown()

        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    @property
    def session(self):
        """
        The primary session for this `CassandraCluster` object

        If the primary session is already set, this property reflects that
        value.  Otherwise, it will try to connect to the keyspace defined in
        `app.config['CASSANDRA_KEYSPACE']` (so it must be set.)
        """

        return self.connect(keyspace=self.keyspace)

    def get_session(self, keyspace):
        """
        Get a session for a specific keyspace

        :param keyspace: the name of a keyspace, or `None`
        :type keyspace: str, None
        """

        return self.connect(keyspace=keyspace)

    def prepare_query(self, query_name, query, keyspace=None, force=False):
        """
        Prepare a query in the session of this `CassandraCluster` object

        :param query_name: the name of the query.  `execute_prepared` can
                           use this name to execute a prepared query
        :param query: a query written in the CQL language
        :type query_name: str
        :type query: str
        :returns: a Cassandra prepared query.  This can be used explicitly
                  with `session.execute()`
        :rtype: Cassandra.query.PreparedStatement
        """

        keyspace = keyspace or self.keyspace
        session = self.connect(keyspace=keyspace)

        if keyspace not in self.__queries:
            self.__queries[keyspace] = {}

        if force or query_name not in self.__queries[keyspace]:
            self.__queries[keyspace][query_name] = session.prepare(query)

        return self.__queries[keyspace][query_name]

    def execute_prepared(self, query_name,
                         keyspace=None,
                         query=None,
                         params=[]):
        """
        Execute a named query that has been prepared with `prepare_query`

        :param query_name: The name that was used in a call to
                           `prepare_query`
        :param query: A CQL query.  If no query with the name `query_name`
                      was prepared, it will prepare this query with
                      `query_name`.  If a query with `query_name` is already
                      prepared, this argument is ignored.
        :param params: Parameters to pass to the query.
        :type query_name: str
        :type query: str
        :type params: list
        :returns: the result of the query execution
        :rtype: cassandra.cluster.ResultSet
        :raises ValueError: if there is no query prepared with `query_name`,
                             and `query` is not specified
        """

        keyspace = keyspace or self.keyspace
        session = self.connect(keyspace=keyspace)

        if keyspace not in self.__queries:
            self.__queries[keyspace] = {}

        if query_name not in self.__queries[keyspace]:
            if query is None:
                raise ValueError('No query prepared with name "{}" and '
                                 'query is not specified.'.format(query_name))
            else:
                self.prepare_query(query_name, query, keyspace=keyspace)

        return session.execute(self.__queries[query_name], params)

    def connect(self, keyspace=None, level=None):
        keyspace = keyspace or self.keyspace

        # If we already have a Cluster object, reuse it.  Otherwise, create
        # a new one.
        if self.cluster is None:
            self.cluster = Cluster(
                self.nodes,
                self.port)
            # If there is authentication data in the config, let’s use it
            # FIXME: this may not be compatible with older Cassandra
            # versions, but do we really want to support that?
            if self.user and self.password:
                self.cluster.auth_provider = PlainTextAuthProvider(
                    self.user, self.password)

        if not keyspace:
            return None

        # If we already have a session to a keyspace, reuse it.
        if keyspace not in self.__session:
            self.app.logger.debug(
                "Connecting to Cassandra cluster at {}"
                .format(self.nodes))

            self.__session[keyspace] = self.cluster.connect(keyspace=keyspace)

            if level:
                self.__session[keyspace].default_consistency_level = level

        return self.__session[keyspace]

    def teardown(self, exception):
        ctx = stack.top

        if hasattr(ctx, 'cassandra_cluster'):
            ctx.cassandra_cluster.shutdown()

    @property
    def connection(self):
        ctx = stack.top

        if ctx is not None:
            if not hasattr(ctx, 'cassandra_cluster'):
                self.connect()
                ctx.cassandra_cluster = self.cluster

            return ctx.cassandra_cluster

    def __repr__(self):
        auth_data = []

        if self.user or self.password:
            auth_data.append(self.user or '')
            if self.password:
                auth_data.append(self.password)

        conn_string = 'cql://'

        if self.user:
            conn_string += self.user

        if self.password:
            conn_string += ':' + self.password

        hosts = ', '.join(self.nodes)

        if len(self.nodes) != 1:
            hosts = '[{}]'.format(hosts)

        conn_string += hosts

        if self.port:
            conn_string += str(self.port)

        if self.keyspace:
            conn_string += '/' + self.keyspace

        return '<CassandraCluster {}>'.format(conn_string)
