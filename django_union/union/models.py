from django.db import models
from .managers import UnionManager

# Create your models here.
class TestModel(models.Model):
    text = models.CharField(max_length=40)

    objects = UnionManager()

    class Meta(object):
        managed = False
