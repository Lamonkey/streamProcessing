from django.db import models

# Create your models here.
class Flight_Ticket(models.Model):
    ticket_no = models.IntegerField()
    flight_id = models.IntegerField()
    fare_condition = models.CharField(max_length=64)
    amount = models.FloatField()
