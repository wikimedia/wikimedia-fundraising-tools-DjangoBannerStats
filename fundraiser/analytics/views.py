from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext

# Create your views here.

def hello_world(request):

#    return HttpResponse("Hello World")
    return render_to_response(
        "wmf_base.html",
        {'text':"Hello World"},
        context_instance=RequestContext(request)
    )