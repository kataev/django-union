import re
import copy

from django.db import models
from django.db.models.query import QuerySet, RawQuerySet
from django.db.models.options import Options
from django.core.management import color
from django.utils.crypto import get_random_string
from django.db import connections


class UnionError(Exception):
    message = None

    def __init__(self, message):
        self.message = message


column_regex = re.compile('["\'][A-Za-z0-9]*[\'"].["\']([A-Za-z0-9]*)[\'"]')


class UnionRawQuerySet(RawQuerySet):
    @property
    def columns(self):
        columns = super(UnionRawQuerySet, self).columns

        def f(col):
            res = column_regex.findall(col)
            if res:
                return res[0]
            else:
                return col

        return map(f, columns)


def format_sql_print(sql):
    import sqlparse
    print
    print sqlparse.format(sql, reindent=True, keyword_case='upper')


class UnionQuerySet(QuerySet):
    _tables = ()
    _inner = None
    _outer = None
    _qn = None
    _wrappers = {'sqlite': '{}'}

    cource = str
    filter_func = None
    sorting = list

    def split(self, *tables, **kwargs):
        tables_coerce = kwargs.get('coerce', self.cource)
        tables_filter = kwargs.get('filter', self.filter_func)
        table_sort = kwargs.get('sorting', self.sorting)
        if len(tables) == 0:
            tables = kwargs.get('tables')
            if not tables:
                raise UnionError('No tables selected')
            if callable(tables):
                tables = tables()

        self._tables = table_sort(filter(tables_filter, map(tables_coerce, tables)))
        self._inner = self.order_by()
        return self.all()

    def _clone(self, klass=None, setup=False, **kwargs):
        clone = super(UnionQuerySet, self)._clone(klass, setup, **kwargs)
        clone._inner = self._inner
        clone._tables = self._tables
        clone._outer = self._outer
        return clone

    def _sql(self, inner_table):
        # self._inner.query = copy.deepcopy(self._inner.query)
        self._inner.query.join((None, inner_table, None))
        print self._inner.query.join_map
        m = self._inner.query.join_map[(None, inner_table, None)]
        self._inner.query.join_map = {(None, inner_table, None):m}
        return self._inner.query.sql_with_params()

    def _union_as_sql(self, operand):
        connection = connections[self.db]
        separator = ' %s ' % operand

        sql, params = zip(*(self._sql(inner_table_name) for inner_table_name in self._tables))
        # import ipdb; ipdb.set_trace()
        wrapper = self._wrappers.get(connection.vendor, '({})')

        inner_sql = separator.join(wrapper.format(s) for s in sql)
        inner_params = tuple(item for param in params for item in param)  # flattening
        return inner_sql, inner_params

    def _fetch(self, operand, cursor=False):
        if self._inner is None:
            raise UnionError('Fetch without union')

        connection = connections[self.db]
        self._qn = connection.ops.quote_name

        inner_sql, inner_params = self._union_as_sql(operand)

        random_table_name = get_random_string(6)
        final_sql, params = self._sql(random_table_name)

        select = '(%s)'

        random_table_name = self._qn(random_table_name)
        final_sql = final_sql.replace(random_table_name, select % inner_sql)

        format_sql_print(final_sql)

        params = list(params)
        params.extend(inner_params)

        if not cursor:
            return self.model.objects.raw(final_sql, params)
        else:
            cursor = connection.cursor()
            cursor.execute(final_sql, params)
            return cursor

    def union(self, **kwargs):
        return self._fetch(operand='UNION', **kwargs)

    def union_all(self, **kwargs):
        return self._fetch(operand='UNION ALL', **kwargs)

    def using(self, alias):
        return super(UnionQuerySet, self).using(alias)


class UnionManager(models.Manager):
    def __getattr__(self, attr, *args):
        if attr.startswith("_"):
            raise AttributeError
        return getattr(self.get_query_set(), attr, *args)

    def get_query_set(self):
        return UnionQuerySet(self.model, using=self._db)

    def get_model(self, name, module='', **kwargs):
        kwargs.setdefault('db_table', name)
        meta = type('Meta', (Options,), kwargs)
        attrs = {'__module__': module, 'Meta': meta, 'objects': UnionManager()}
        attrs.update([(f.name, f) for f in self.model._meta.fields])
        return type(name, (models.Model,), attrs)

    def create_sql(self, model):
        style = color.no_style()
        connection = connections[self.db]
        return connection.creation.sql_create_model(model, style)

    def raw(self, raw_query, params=None, *args, **kwargs):
        return UnionRawQuerySet(raw_query=raw_query, model=self.model, params=params, using=self._db, *args, **kwargs)
