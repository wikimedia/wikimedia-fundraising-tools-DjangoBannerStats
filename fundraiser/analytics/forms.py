from django import forms

class CampaignForm(forms.Form):
    campaign = forms.CharField(required=True)

    def clean_campaign(self):
        import re
        campaign = self.cleaned_data['campaign']
        if re.match(r"^([0-9a-zA-Z_-]+)$", campaign):
            return campaign
        else:
            raise forms.ValidationError("Invalid campaign name")

