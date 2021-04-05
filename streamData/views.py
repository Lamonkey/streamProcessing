from typing import Reversible
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.http import HttpResponse
from django import forms
from django.urls import reverse
# Create your views here.
class NewDataForm(forms.Form):
    data = forms.CharField(label="New Data")

def index(request):
    return HttpResponse("Hello, world!")

def streamData(request,rate):
    return render(request,'streamData/template.html',{
        "rate":rate
    })
data = []
def displayData(request):
    return render(request,"streamData/displayData.html",{
        "data":data
    })

def addData(request):
    if request.method == "POST":
        form = NewDataForm(request.POST)
        if form.is_valid():
            newData = form.cleaned_data["data"]
            data.append(newData)
            return HttpResponseRedirect(reverse("streamData:displayData"))
        else:
            return render(request,"streamData/addData.html",{
                "form":form
            })
    return render(request,"streamData/addData.html",{
        "form":NewDataForm()
    })
   
    
    