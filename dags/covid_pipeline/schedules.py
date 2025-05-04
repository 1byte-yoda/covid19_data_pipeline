from dagster import define_asset_job, AssetSelection, ScheduleDefinition, load_assets_from_modules

from .assets import covid_datahub, covid19_github_csse, dbt_core

covid_assets = load_assets_from_modules([covid_datahub, covid19_github_csse, dbt_core])
covid_data_etl_job = define_asset_job(name="covid_data_etl_job", selection=AssetSelection.assets(*covid_assets))

synthetic_back_fill_job = define_asset_job(name="synthetic_backfill_job", selection=AssetSelection.keys("register_existing_s3_data",))
synthetic_back_fill_schedule = ScheduleDefinition(job=synthetic_back_fill_job, cron_schedule="0 2 * * *")

covid_data_etl_schedule = ScheduleDefinition(job=covid_data_etl_job, cron_schedule="0 2 * * *")

schedules = [covid_data_etl_schedule, synthetic_back_fill_schedule]
