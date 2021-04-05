from django.urls import path
from . import views
app_name = "streamData"
urlpatterns = [
    path("",views.index, name = "index"),
    path("streamData/<int:rate>",views.streamData,name="streamData"),
    path("displayData/",views.displayData,name="displayData"),
    path("addData/",views.addData,name="addData"),
]