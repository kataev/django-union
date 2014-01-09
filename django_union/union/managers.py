from django.db import models
from django.db.models.query import QuerySet
from django.db.models.options import Options
from django.core.management import color
from django.db import connections


class UnionError(Exception):
    message = None

    def __init__(self, message):
        self.message = message


class UnionQuerySet(QuerySet):
    _tables = ()
    _inner = None
    _outer = None

    def _sql(self, inner_table):
        table_name = self._inner.model._meta.db_table
        alias_data = self._inner.query.alias_map.pop(table_name)
        inner_table_name = '"%s" AS "%s"' % (inner_table, table_name)
        self._inner.query.alias_map[table_name] = alias_data._replace(table_name=inner_table_name,
                                                                      rhs_alias=inner_table_name)
        return self._inner.query.sql_with_params()

    def _union_as_sql(self):
        for inner_table_name in self._tables:
            yield self._sql(inner_table_name)

    def union(self, *tables, **kwargs):
        tables_coerce = kwargs.get('coerce', str)
        tables_filter = kwargs.get('filter', None)
        table_sort = kwargs.get('sorted', list)
        if len(tables) == 0:
            tables = kwargs.get('tables')
            if not tables:
                raise UnionError('No tables selected')
            if callable(tables):
                pass

        self._tables = table_sort(filter(tables_filter, map(tables_coerce, tables)))
        self._inner = self
        return self._clone()

    def _clone(self, klass=None, setup=False, **kwargs):
        clone = super(UnionQuerySet, self)._clone(klass, setup, **kwargs)
        clone._inner = self._inner
        clone._tables = self._tables
        clone._outer = self._outer
        return clone

    def fetch(self, cursor=False):
        if self._inner is None:
            raise UnionError('Fetch without union')
        connection = connections[self.db]

        sql, params = zip(*self._union_as_sql())

        if connection.vendor == 'sqlite':
            union_sql = ' UNION ALL '.join(sql)
        else:
            union_sql = ' UNION ALL '.join('(%s)' % s for s in sql)

        if not cursor:
            return self.model.objects.raw(union_sql, *params)
        else:
            cursor = connection.cursor()
            cursor.execute(union_sql, *params)
            return cursor

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
        attrs = {'__module__': module, 'Meta': meta}
        attrs.update([(f.name, f) for f in self.model._meta.fields])
        return type(name, (models.Model,), attrs)

    def create_sql(self, model):
        style = color.no_style()
        connection = connections[self.db]
        return connection.creation.sql_create_model(model, style)
