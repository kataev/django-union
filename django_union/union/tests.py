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


class UnionTest(TestCase):
    def setUp(self):
        self.model = TestModel
        self.connection = connections[TestModel.objects.db]


    def test_simple(self):
        queryset = self.model.objects.filter(text='filter').union(2013, 2014)
        self.assertIsNotNone(queryset._inner)
        self.assertIsNotNone(queryset._tables)

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

    def test_fetch(self):
        name = 'asd1'
        model = self.dynamic_model(name)

        for x in range(10):
            m = model(text='filter')
            m.save()

        name1 = 'asd2'
        model = self.dynamic_model(name1)
        for x in range(10):
            m = model(text='filter')
            m.save()
        self.assertEqual(model.objects.count(), 10)
        self.assertTrue(name in self.connection.introspection.table_names())
        queryset = self.model.objects.filter(text='filter').union(name, name1).all()

        query = queryset.fetch()
        i = 0
        for i, x in enumerate(query):
            print i, x.text

        self.assertEqual(i, 19)

        query = queryset.fetch(cursor=True)

        for i, x in enumerate(query):
            print i, x[1]

        self.assertEqual(i, 19)
