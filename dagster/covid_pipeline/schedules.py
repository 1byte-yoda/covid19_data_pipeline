from dagster import define_asset_job, AssetSelection, ScheduleDefinition

covid_data_etl_job = define_asset_job(name="covid_data_etl_job", selection=AssetSelection.all())
covid_data_etl_schedule = ScheduleDefinition(job=covid_data_etl_job, cron_schedule="0 2 * * *")

schedules = [covid_data_etl_schedule]