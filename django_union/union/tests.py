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
    def setUp(self):
        self.model = TestModel
        self.connection = connections[TestModel.objects.db]


    def test_simple(self):
        queryset = self.model.objects.filter(text='filter').split(2013, 2014)
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
        names = ('asd1', 'asd2')
        for name in names:
            model = self.dynamic_model(name)

            for x in range(10):
                m = model(text='filter')
                m.save()

            self.assertEqual(model.objects.count(), 10)

        self.assertTrue(all(name in self.connection.introspection.table_names() for name in names))
        queryset = self.model.objects.filter(text='filter').split(*names).filter(id=1)

        # query = queryset.union_all()
        # self.assertEqual(len(tuple(r for r in query)), 20)
        queryset = queryset.annotate(asd=models.Count('id'))
        query = queryset.union_all(cursor=True)
        query = tuple(query)
        self.assertEqual(len(tuple(r for r in query)), 20)
        print query
        # queryset = self.model.objects.filter(text='filter').split(*names).all()
        #
        # query = queryset.union_all()
        # self.assertEqual(len(tuple(query)), 20)
        #
        # query = queryset.union()
        # self.assertEqual(len(tuple(query)), 20)
