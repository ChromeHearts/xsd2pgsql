#! /usr/bin/python
""" xsd2pgsql.py
========================================

Create a database based on an XSD schema. 

Usage
========================================
    <file>  XSD file to base the Postgres Schema on

    -f  --fail  Fail on finding a bad XS type
    -a  --as-is     Don't normalize element names.

    -d  --database  DB name
    -u  --user  DB Username
    -p  --password  DB Password
    -h  --host  DB host
    -P  --port  DB port
"""
import sys
import logbook
import lxml
from logbook import StreamHandler

logger = logbook.Logger('xsd2pgsql')

""" You can set default DB connection settings here if you'd like
"""
DB = {
    'user': None,
    'pass': None,
    'port': 5432,
    'host': 'localhost',
}

""" Some configuration items """
MAX_RECURSE_LEVEL = 100

""" XSD to Postgres data type translation dictionary. 
"""


class SDict(dict):
    def __getitem__(self, item):
        return dict.__getitem__(self, item) % self

    def get(self, item):
        try:
            return dict.__getitem__(self, item) % self
        except KeyError:
            return None


DEFX2P = SDict({
    'string': 'varchar',
    'boolean': 'boolean',
    'decimal': 'numeric',
    'float': 'real',
    'double': 'double precision',
    'duration': 'interval',
    'dateTime': 'timestamp',
    'time': 'time',
    'date': 'date',
    'gYearMonth': 'timestamp',
    'gYear': 'timestamp',
    'gMonthDay': 'timestamp',
    'gDay': 'timestamp',
    'gMonth': 'timestamp',
    'hexBinary': 'bytea',
    'base64Binary': 'bytea',
    'anyURI': 'varchar',
    'QName': None,
    'NOTATION': None,
    'normalizedString': '%(string)s',
    'token': '%(string)s',
    'language': '%(string)s',
    'NMTOKEN': None,
    'NMTOKENS': None,
    'Name': '%(string)s',
    'NCName': '%(string)s',
    'ID': None,
    'IDREF': None,
    'IDREFS': None,
    'ENTITY': None,
    'ENTITIES': None,
    'integer': 'integer',
    'nonPositiveInteger': '%(integer)s',
    'negativeInteger': '%(integer)s',
    'long': '%(integer)s',
    'int': '%(integer)s',
    'short': '%(integer)s',
    'byte': '%(integer)s',
    'nonNegativeInteger': '%(integer)s',
    'unsignedLong': '%(integer)s',
    'unsignedInt': '%(integer)s',
    'unsignedShort': '%(integer)s',
    'unsignedByte': '%(integer)s',
    'positiveInteger': '%(integer)s',
})
USER_TYPES = {}

XMLS = "{http://www.w3.org/2001/XMLSchema}"
XMLS_PREFIX = "xs:"

""" Output """
SQL = ''

""" Helpers
"""


class InvalidXMLType(Exception): pass


class MaxRecursion(Exception): pass


""" Normalize strings like column names for PG """


def pg_normalize(string):
    if not string: string = ''
    string = string.replace('-', '_')
    string = string.replace('.', '_')
    string = string.replace(' ', '_')
    string = string.lower()
    return string


""" Look for elements recursively

    returns tuple (children bool, sql string)
"""


def look4element(root, ns, el, parent=None, recurse_level=0, fail=False, normalize=True):
    nsd = {'r': 'http://www.w3.org/2001/XMLSchema'}
    if recurse_level > MAX_RECURSE_LEVEL: raise MaxRecursion()
    cols = None
    children = False
    sql = ''
    for x in el.findall(ns + 'element'):
        children = True

        rez = look4element(root, ns, x, x.get('name') or parent, recurse_level + 1, fail=fail)
        logger.debug("name: {}, rez: {}", x.get('name'), rez)
        sql += rez[1] + '\n'
        if not rez[0]:

            # print 'parent(%s) <%s name=%s type=%s> %s' % (parent, x.tag, x.get('name'), x.get('type'), x.text)

            thisType = x.get('type') or x.get('ref') or 'string'
            k = thisType.replace(XMLS_PREFIX, '')

            pgType = DEFX2P.get(k) or USER_TYPES.get(k) or None
            if not pgType:
                name = k.replace('dlican:', '')
                type_node = root.find("r:complexType[@name='{}']".format(name), nsd)
                if type_node:
                    rez = look4element(root, ns, type_node, type_node.get('name'), recurse_level + 1, fail=fail)
                    sql += rez[1] + '\n'
            elif pgType:
                colName = x.get('name') or x.get('ref')
                if normalize:
                    colName = pg_normalize(colName)

                if not cols:
                    cols = "%s %s" % (colName, pgType)
                else:
                    cols += ", %s %s" % (colName, pgType)
            # cols += ''

    if cols:
        sql += """CREATE TABLE %s (%s);""" % (parent, cols)
    for x in el.findall(ns + 'complexType'):
        children = True
        rez = look4element(root, ns, x, x.get('name') or parent, recurse_level + 1, fail=fail)
        sql += rez[1] + '\n'
    for x in el.findall(ns + 'sequence'):
        children = True
        rez = look4element(root, ns, x, x.get('name') or parent, recurse_level + 1, fail=fail)
        sql += rez[1] + '\n'
    return (children, sql)


""" Take care of any types that were defined in the XSD """


def buildTypes(ns, root_element):
    """
    To get all the simple element and simpletype that are readily translated into psql native type
    :param ns:
    :param root_element:
    :return:
    """
    logger.debug("ns = {}", ns)
    nsd = {'r': ns}
    for el in root_element.findall('r:element', nsd):
        logger.debug('all element: name: {}', el.get('name'))
        if el.get('name') and el.get('type'):
            USER_TYPES[pg_normalize(el.get('name'))] = DEFX2P.get(el.get('type').replace(XMLS_PREFIX, ''))
            logger.debug("element: name={}, type={}", pg_normalize(el.get('name')), USER_TYPES[pg_normalize(el.get('name'))])

    for el in root_element.findall('r:simpleType', nsd):
        restr = el.find('r:restriction', nsd)
        USER_TYPES[pg_normalize(el.get('name'))] = restr.get('base').replace(XMLS_PREFIX, '')
        logger.debug("simpleType: name={}, type={}", pg_normalize(el.get('name')), USER_TYPES[pg_normalize(el.get('name'))])


def main():
    """ Imports
    """
    import argparse, psycopg2
    import pyxb.utils.domutils as domutils
    from lxml import etree

    """ Handle options
    """
    parser = argparse.ArgumentParser(
        description='Create a database based on an XSD schema.  If no database name is specified, SQL is output to stdout.')
    parser.add_argument(
        'xsd',
        metavar='FILE',
        type=lambda x: open(x, 'r'),
        nargs='+',
        help='XSD file to base the Postgres Schema on',
    )
    parser.add_argument(
        '-f', '--fail',
        dest='failOnBadType',
        action='store_true',
        default=False,
        help='Fail on finding a bad XS type'
    )
    parser.add_argument(
        '-a', '--as-is',
        dest='as_is',
        action='store_true',
        default=False,
        help="Don't normalize element names"
    )
    parser.add_argument(
        '-d', '--database',
        metavar='NAME',
        dest='db_name',
        type=str,
        nargs='?',
        help='DB Name'
    )
    parser.add_argument(
        '-u', '--user',
        metavar='USERNAME',
        dest='db_username',
        type=str,
        nargs='?',
        help='DB Username'
    )
    parser.add_argument(
        '-p', '--password',
        metavar='PASSWORD',
        dest='db_password',
        type=str,
        nargs='?',
        help='DB Password'
    )
    parser.add_argument(
        '-n', '--host',
        metavar='HOSTNAME',
        dest='db_host',
        type=str,
        nargs='?',
        default='localhost',
        help='DB Host'
    )
    parser.add_argument(
        '-P', '--port',
        metavar='PORT',
        dest='db_port',
        type=int,
        nargs='?',
        default=5432,
        help='DB Port (Default: 5432)'
    )
    args = parser.parse_args()

    """ MEAT
    """
    if not args.xsd:
        sys.exit('XSD file not specified.')
    else:
        for f in args.xsd:
            """ Parse the XSD file
            """
            xsd = etree.parse(f)

            # glean out defined tyeps
            buildTypes("http://www.w3.org/2001/XMLSchema", xsd)
            # buildTypes("https://services.bloomberg.com/datalic/cans/20121022/cans.xsd", xsd)

            # parse structure
            if args.as_is:
                norm = False
            else:
                norm = True
            result = look4element(xsd, XMLS, xsd, pg_normalize(f.name.split('.')[0]), fail=args.failOnBadType,
                                  normalize=norm)
        if result[1] and not args.db_name:
            print(result[1].replace('\n\n', ''))
        elif result[1]:
            dsn = "dbname=%s host=%s port=%s" % (args.db_name, args.db_host, args.db_port)
            if args.db_username: dsn += "user=%s" % args.db_username
            if args.db_username: dsn += "user=%s" % args.db_password
            conn = psycopg2.connect(dsn)
            cur = conn.cursor()
            cur.execute(result[1])
            conn.commit()
            cur.close()
            conn.close()
        else:
            raise Exception("This shouldn't happen.")


""" Do it
"""
if __name__ == '__main__':
    st_h = StreamHandler(sys.stdout, level=logbook.DEBUG)
    with st_h.applicationbound():
        logger.info("Start ...")
        main()
