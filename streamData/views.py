from typing import Reversible
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.http import HttpResponse
from django import forms
from django.urls import reverse
from django.forms import ModelForm
from streamData.models import Flight_Ticket
# Create your views here.
class FlightTicketForm(ModelForm):
    class Meta:
        model = Flight_Ticket
        fields = ['ticket_no','flight_id','fare_condition','amount']

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
        form = FlightTicketForm(request.POST)
        if form.is_valid():
            #ticket = form.cleaned_data["FlightTicketForm"]
            #data.append(ticket)
            form.save()
            return HttpResponseRedirect(reverse("streamData:displayData"))
        else:
            return render(request,"streamData/addData.html",{
                "form":form
            })
    return render(request,"streamData/addData.html",{
        "form":FlightTicketForm()
    })
   
    
    