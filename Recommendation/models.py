from django.db import models

# Create your models here.
class PoemData(models.Model):
    poemId = models.IntegerField()
    poemDynasty = models.CharField(max_length=10)
    poemTagNames = models.CharField(max_length=100)
    poemStar = models.IntegerField()