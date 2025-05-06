from dagster import ScheduleDefinition

from .jobs import covid_data_etl_job

covid_data_etl_schedule = ScheduleDefinition(job=covid_data_etl_job, cron_schedule="0 2 * * *")

schedules = [covid_data_etl_schedule]
