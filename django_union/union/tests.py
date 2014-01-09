from django.test import TestCase
from django.db import connections

from .models import TestModel


class UnionTest(TestCase):
    def setUp(self):
        self.model = TestModel

    def test_simple(self):
        queryset = self.model.objects.filter(text='filter').union(2013, 2014)
        self.assertIsNotNone(queryset._inner)
        self.assertIsNotNone(queryset._tables)
        # queryset.fetch()

    def dynamic_model(self, name='asd'):
        model = TestModel.objects.get_model(name, app_label='fake_app_label')
        connection = connections[TestModel.objects.db]
        cursor = connection.cursor()

        statements, pending = TestModel.objects.create_sql(model)
        old_length = len(connection.introspection.table_names())
        for sql in statements:
            cursor.execute(sql, pending)

        new_length = len(connection.introspection.table_names())
        self.assertEqual(old_length + 1, new_length)
        return model

    def test_fetch(self):
        name = 'asd1'
        model = self.dynamic_model(name)

        for x in range(10):
            m = model(text='filter')
            m.save()
        self.assertEqual(model.objects.count(), 10)
        connection = connections[TestModel.objects.db]
        self.assertTrue(name in connection.introspection.table_names())
        queryset = self.model.objects.filter(text='filter').union(name).all()

        query = queryset.fetch()

        for i, x in enumerate(query):
            print i, x.text

        self.assertEqual(len(tuple(query)), 10)