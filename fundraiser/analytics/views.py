from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext

import fundraiser.analytics.forms as forms

# Create your views here.

def hello_world(request):

#    return HttpResponse("Hello World")
    return render_to_response(
        "wmf_base.html",
        {'text':"Hello World"},
        context_instance=RequestContext(request)
    )

def campaign_ecom(request):
    if request.method == 'POST': # If the form has been submitted...
        form = forms.CampaignForm(request.POST) # A form bound to the POST data
        if form.is_valid(): # All validation rules pass
            return render_to_response(
                "analytics/campaign_ecom.html",
                { 'form' : form,
                  'campaign' : form.cleaned_data['campaign']
                },
                context_instance=RequestContext(request)
            )
    else:
        form = forms.CampaignForm() # An unbound form

    return render_to_response(
        "analytics/campaign_ecom.html",
        { 'form' : form },
        context_instance=RequestContext(request)
    )