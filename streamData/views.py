from typing import Reversible
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.http import HttpResponse
from django.forms import ModelForm
from django.urls import reverse
from .models import Flight, Ticket
from django import forms
# Create your views here.
# class NewDataForm(ModelForm):
#     class Meta:
#         model = Flight
#         fields = ['origin','destination','duration']

class multiTickets(forms.Field):
    def to_python(self,value):
        if not value:
            return[]
        return value.split('\n')
    def validate(self,value):
        super().validate(value)
        for flight in value:
            if len(flight) != 3:
                return False

class NewDataForm(forms.Form):
    data = multiTickets(label="data",widget=forms.Textarea)
   


    


def index(request):
    return render(request,'streamData/index.html')

def streamData(request,rate):
    return render(request,'streamData/template.html',{
        "rate":rate
    })

def displayData(request):
    start = int(request.GET.get("start") or 0)
    end = int(request.GET.get('end') or (start + 9))
    return render(request,"streamData/displayData.html",{
        "data":Ticket.objects.all()[start:end]
    })

def addData(request):
    if request.method == "POST":
        form = NewDataForm(request.POST)
        #print(request.POST)
        if form.is_valid():
            newData = form.cleaned_data["data"]
            for line in newData:
                p = line.split(',')
                created = Ticket(
                    ticket_no=p[0],
                    flight_id=p[1],
                    fare_conditions=p[2],
                    amount = p[3]
                )
                created.save()
                
            return HttpResponseRedirect(reverse("streamData:displayData"))
        else:
            return render(request,"streamData/addData.html",{
                "form":form
            })
    return render(request,"streamData/addData.html",{
        "form":NewDataForm()
    })
   
    
    