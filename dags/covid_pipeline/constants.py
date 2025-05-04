import os

SLACK_HOOK = os.environ.get("SLACK_HOOK_URL")
GITHUB_COVID_URL = "https://raw.githubusercontent.com/cssegisanddata/covid-19/refs/heads/master/csse_covid_19_data/csse_covid_19_daily_reports/{query_date}.csv"