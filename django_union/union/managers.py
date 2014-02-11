# coding: utf-8
import re

from contextlib import contextmanager

from django.db import models
from django.db.models.query import QuerySet, RawQuerySet
from django.db.models.options import Options
from django.core.management import color
from django.utils.crypto import get_random_string
from django.db import connections


@contextmanager
def patch_db_table(query, name):
    meta = query.get_meta()
    db_table = meta.db_table
    meta.db_table = name
    yield
    meta.db_table = db_table


class UnionError(Exception):
    message = None

    def __init__(self, message):
        self.message = message


column_regex = re.compile('^"\w"."(\w)"')


class UnionRawQuerySet(RawQuerySet):
    @property
    def columns(self):
        columns = super(UnionRawQuerySet, self).columns

        def f(col):
            if '.' in col:
                return col.split('.')[1].strip('"')
            # res = column_regex.findall(col)
            # print res
            # if res:
            #     return res[0]
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

    _wrappers = {'sqlite': '{}'}

    cource = str
    filter_func = None
    sorting = list

    @property
    def _qn(self):
        return connections[self.db].ops.quote_name

    def split(self, *tables, **kwargs):
        tables_coerce = kwargs.get('coerce', self.cource)
        tables_filter = kwargs.get('filter', self.filter_func)
        table_sort = kwargs.get('sorting', self.sorting)
        if callable(tables):
            tables = tables()
        if len(tables) == 0:
            tables = kwargs.get('tables')
        if not tables:
            raise UnionError('No tables selected')

        self._tables = table_sort(filter(tables_filter, map(tables_coerce, tables)))
        self._inner = self.order_by()
        return self.all()

    def _clone(self, klass=None, setup=False, **kwargs):
        clone = super(UnionQuerySet, self)._clone(klass, setup, **kwargs)
        clone._inner = self._inner
        clone._tables = self._tables
        clone._outer = self._outer
        return clone

    def _sql(self, table):
        '''
         Нам для внешнего запроса надо сделать alias
         " from 'randomstring' as 'true name' "
         чтобы потом заменить рандомную строку на наш запрос
         т.е в алиасе должно быть нормальное имя а в настоящем рандом строка.
         '''
        query = self.query
        if table in self._tables:
            query = self._inner.query
        else:
            print table
            table_name = query.get_meta().db_table
            alias_data = query.alias_map.pop(table_name)
            alias_data = alias_data._replace(table_name=table_name)  #, rhs_alias=table_name)
            query.alias_map[table_name] = alias_data
            # print query.alias_map
            # name, alias, join_type, lhs, join_cols, _, join_field = self.query.alias_map[alias]
        # query =
        # print table
        # print query.tables, query.table_map, query.alias_map
        # print query.__dict__
        # print query.get_compiler('default').__dict__

        if table in self._tables:
            query.tables = []
            with patch_db_table(query, table):
                return query.sql_with_params()
        else:
            return query.sql_with_params()

    def _union_as_sql(self, operand):
        connection = connections[self.db]
        separator = ' %s ' % operand

        sql, params = zip(*(self._sql(inner_table_name) for inner_table_name in self._tables))
        wrapper = self._wrappers.get(connection.vendor, '({})')

        inner_sql = separator.join(wrapper.format(s) for s in sql)
        inner_params = tuple(item for param in params for item in param)  # flattening
        return inner_sql, inner_params

    def _fetch(self, operand, cursor=False):
        if self._inner is None:
            raise UnionError('Fetch without union')

        random_table_name = get_random_string(6)

        inner_sql, inner_params = self._union_as_sql(operand)
        outer_sql, outer_params = self._sql(random_table_name)
        # format_sql_print(outer_sql)
        final_sql = outer_sql.replace(self._qn(random_table_name) + ' ',
                                      '(%s) AS ' % inner_sql)
        final_params = tuple(outer_params + inner_params)

        format_sql_print(final_sql)

        if cursor:
            cursor = connections[self.db].cursor()
            cursor.execute(final_sql, final_params)
            return cursor

        return self.model.objects.raw(final_sql, final_params)


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
