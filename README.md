wmf_fundraiser_django
=====================

# This is how you rescan everything in the banner log directory
export PYTHONPATH=/etc/fundraising
/srv/DjangoBannerStats/manage.py LoadLPImpressions --verbose --file="/srv/archive/banner_logs/2023/landingpages.tsv.*"
/srv/DjangoBannerStats/manage.py LoadBannerImpressions2Aggregate --verbose --top --file="/srv/archive/banner_logs/2023/beacon*"

# This is what we run on a 15-minute interval via process-control
export PYTHONPATH=/etc/fundraising
/srv/DjangoBannerStats/manage.py LoadLPImpressions --verbose --recent
/srv/DjangoBannerStats/manage.py LoadBannerImpressions2Aggregate --verbose --top --recent
