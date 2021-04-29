from typing import Reversible
from django.core import exceptions
from django.core.exceptions import ValidationError
from django.forms.widgets import Textarea
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.http import HttpResponse
from django import forms
from django.urls import reverse
from django.forms import ModelForm
from streamData.models import Flight_Ticket
from .refractor_pipeline import PipelineRefractor

class UploadFileForm(forms.Form):
    #title = forms.CharField(max_length=50)
    file = forms.FileField(
        label='Select a file',
        help_text='max. 42 megabytes'
    )

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

def read_file(f):
    #read from form
    lines = [(chr(line)) for line in f.read()]
    lines = "".join(lines)
    #write file
    with open('batchPipline.txt','w') as file:
        file.write(lines)
    return lines

#refactoring uploaded batch to stream
def convert(request):
    if request.method == 'POST':
        #form = UploadFileForm(request.POST)
        form = UploadFileForm(request.POST,request.FILES)
        #return HttpResponse(form, content_type="text/plain")
        if form.is_valid():
            #print("valid")
            #file = {}
            batchFile = read_file(request.FILES['file'])
            refactor = PipelineRefractor("batchPipline.txt","streamPipline.txt","template.txt")
            refactor.refractor()
            with open('streamPipline.txt','r') as streamFileSource:
                streamFile = streamFileSource.readlines()
            streamFile = "".join(streamFile)
            #print(len(file))
            return render(request,'streamData/convert.html',{
                "batchFile":batchFile,"streamFile":streamFile
                })
            #return HttpResponseRedirect('streamData/convert.html')
    
    else:
        form = UploadFileForm()
    return render(request, 'streamData/convert.html', {'form': form})
    
    