from django.test import TestCase
from django.db import connections
from django.db import models

from .managers import UnionManager


class TestModel(models.Model):
    text = models.CharField(max_length=40)

    objects = UnionManager()

    class Meta(object):
        managed = False
        app_label = 'union'
        ordering = ('text',)


class UnionTest(TestCase):
    models = []
    names = ('asd1', 'asd2')

    def setUp(self):
        self.connection = connections[TestModel.objects.db]
        self.prepare_models()

    def dynamic_model(self, name='asd'):
        model = TestModel.objects.get_model(name, app_label='fake_app_label')
        cursor = self.connection.cursor()

        statements, pending = TestModel.objects.create_sql(model)
        old_length = len(self.connection.introspection.table_names())
        for sql in statements:
            cursor.execute(sql, pending)

        new_length = len(self.connection.introspection.table_names())
        self.assertEqual(old_length + 1, new_length)
        return model

    def prepare_models(self):
        for name in self.names:
            model = self.dynamic_model(name)
            self.models.append(model)

            for x in range(10):
                m = model(text=name)
                m.save()

            self.assertEqual(model.objects.count(), 10)
            queryset = model.objects.filter(text='filter').split(2013, 2014)
            self.assertIsNotNone(queryset._inner)
            self.assertIsNotNone(queryset._tables)

        self.assertTrue(all(name in self.connection.introspection.table_names() for name in self.names))


class TestCursor(UnionTest):
    def test_fetch(self):
        model = self.models[0]

        queryset = model.objects.filter(text=self.names[0]).split(*self.names).filter(id=1)

        queryset = queryset.annotate(asd=models.Count('id'))
        query = queryset.union_all(cursor=True)
        query = tuple(query)
        self.assertEqual(len(tuple(r for r in query)), 10)

        queryset = model.objects.filter(text=self.names[1]).split(*self.names).filter(id=1)

        queryset = queryset.annotate(asd=models.Count('id'))
        query = queryset.union_all(cursor=True)
        query = tuple(query)
        self.assertEqual(len(tuple(r for r in query)), 10)


class UnionNonCursorTest(UnionTest):
    def test_non_cursor(self):
        model = self.models[0]

        sql, params = model.objects.all().query.sql_with_params()

        for x in model.objects.raw(sql, params):
            self.assertEqual(x.text, x._meta.db_table)

        for name in self.names:
            queryset = model.objects.filter(text=name).split(*self.names).all()
            query = queryset.union_all()
            self.assertEqual(len(tuple(query)), 10)

            query = queryset.union()
            self.assertEqual(len(tuple(query)), 10)

        queryset = model.objects.all().split(*self.names).all()
        query = queryset.union_all()
        self.assertEqual(len(tuple(query)), 20)

        query = queryset.union()
        self.assertEqual(len(tuple(query)), 20)
