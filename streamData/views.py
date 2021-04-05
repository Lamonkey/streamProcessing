from typing import Reversible
from django.core.exceptions import ValidationError
from django.forms.widgets import Textarea
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.http import HttpResponse
from django import forms
from django.urls import reverse
from django.forms import ModelForm
from streamData.models import Flight_Ticket
# Create your views here.
class MultiTicketField(forms.Field):
    def to_python(self,value):
        if not value:
            return []
        return value.split('\n')
    def validate(self,value):
        super().validate(value)
        for ticket in value:
            if (len(ticket.split(','))!=4):
                raise ValidationError()

# class FlightTicketForm(ModelForm):
#     class Meta:
#         model = Flight_Ticket
#         fields = ['ticket_no','flight_id','fare_condition','amount']
    

class TicketsForm(forms.Form):
    tickets = MultiTicketField(widget=Textarea,label="tickets")
    # def clean(self):
    #     cleaned_data = super().clean()
    #     tickets = cleaned_data.get("tickets")
    #     if tickets:
    #         return tickets


def index(request):
    return render(request,'streamData/index.html')

def streamData(request,rate):
    return render(request,'streamData/template.html',{
        "rate":rate
    })
data = []
def displayData(request):
    return render(request,"streamData/displayData.html",{
        "data":Flight_Ticket.objects.all()
    })

def addData(request):
    if request.method == "POST":
        form = TicketsForm(request.POST)
        if form.is_valid():
            tickets = form.cleaned_data["tickets"]
            #print(tickets)
            for line in tickets:
               # print(line)
                p = line.split(',')
                ticket = Flight_Ticket.objects.create(ticket_no=p[0],
                flight_id=p[1],
                fare_condition=p[2],
                amount=p[3])
                ticket.save()
            #ticket = form.cleaned_data["FlightTicketForm"]
            #data.append(ticket)
            
            return HttpResponseRedirect(reverse("streamData:displayData"))
        else:
            return render(request,"streamData/addData.html",{
                "form":form
            })
    return render(request,"streamData/addData.html",{
        "form":TicketsForm
    })
   
    
    