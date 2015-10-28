# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import logging
# from petl.compat import next, text_type, string_types


# internal dependencies
# from petl.errors import ArgumentError
from petl.util.base import Table

# external dependencies
import ldap3


DEFAULTS = {
    'PAGE_SIZE': 500,
}


logger = logging.getLogger(__name__)
debug = logger.debug
warning = logger.warning


def fromldap(connection, base_ou, query, attributes=[], scope=ldap3.SUBTREE, page_size=DEFAULTS['PAGE_SIZE']):
    return LdapView(connection, base_ou, query, attributes, scope, page_size)


class LdapView(Table):
    def __init__(self, connection, base_ou, query, attributes, scope, page_size):
        self.connection = connection
        self.base_ou = base_ou
        self.query = query
        self.attributes = attributes
        self.scope = scope
        self.page_size = page_size

    def __iter__(self):
        return _iter_ldap_query(self.connection, self.base_ou, self.query, self.attributes, self.scope, self.page_size)


def _iter_ldap_query(connection, base_ou, query, attributes, scope, page_size):
    connection.bind()
    connection.search(search_base=base_ou, search_filter=query, search_scope=scope, attributes=attributes, paged_size=page_size, paged_cookie=None)
    logger.debug('Connection.search.response is: {}'.format(connection.response))
    if len(connection.response) < page_size:
        results = connection.response
    else:
        results = connection.response
        cookie = connection.result['controls']['1.2.840.113556.1.4.319']['value']['cookie']
        while cookie:
            connection.search(search_base=base_ou, search_filter=query, search_scope=scope, attributes=attributes, paged_size=page_size, paged_cookie=cookie)
            results += connection.response
            cookie = connection.result['controls']['1.2.840.113556.1.4.319']['value']['cookie']
    connection.unbind()
    # Headers
    yield attributes
    for result in results:
        yield [result[a] for a in attributes]

