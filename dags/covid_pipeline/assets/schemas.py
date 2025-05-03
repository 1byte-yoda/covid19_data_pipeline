from dagster import TableSchema, TableColumn, TableColumnConstraints, TableConstraints


class Covid19CsseGithub:
    @staticmethod
    def dagster_table_schema() -> TableSchema:
        return TableSchema(
            columns=[
                TableColumn(name="row_num", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="row_key", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="fips", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="admin2", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="province_state", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="country_region", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="last_update", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="lat", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="longx", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="confirmed", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="deaths", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="recovered", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="active", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="combined_key", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="incident_rate", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="case_fatality_ratio", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="file_md5", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="source_url", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="id", type="text", description=None, constraints=TableColumnConstraints(nullable=False, unique=False, other=["primary_key"]), tags={}),
                TableColumn(name="load_date", type="timestamp", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["precision", "timezone"]), tags={}),  # noqa
                TableColumn(name="year", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="month", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="day", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            ],
            constraints=TableConstraints(other=[]),
        )

    @classmethod
    def pandas_schema(cls) -> dict[str, str]:
        return convert_to_pandas_dtypes(table_schema=cls.dagster_table_schema())


class Covid19DataHub:
    @classmethod
    def dagster_table_schema(cls) -> TableSchema:
        return TableSchema(
            columns=[
                TableColumn(name="row_num", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="row_key", type="text", description=None, constraints=TableColumnConstraints(nullable=False, unique=False, other=["primary_key"]), tags={}),
                TableColumn(name="id", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="date", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="administrative_area_level", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="country", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="state", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="city", type="text", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="population", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="school_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="workplace_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="cancel_events", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="gatherings_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="transport_closing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="stay_home_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="internal_movement_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="international_movement_restrictions", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="information_campaigns", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="testing_policy", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="contact_tracing", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="facial_coverings", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vaccination_policy", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="elderly_people_protection", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="stringency_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="containment_health_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="economic_support_index", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="confirmed", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="deaths", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="recovered", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="tests", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vaccines", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="people_vaccinated", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="people_fully_vaccinated", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="hosp", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="icu", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="vent", type="double", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=[]), tags={}),
                TableColumn(name="year", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="month", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
                TableColumn(name="day", type="bigint", description=None, constraints=TableColumnConstraints(nullable=True, unique=False, other=["partition"]), tags={}),
            ],
            constraints=TableConstraints(other=[]),
        )

    @classmethod
    def pandas_schema(cls) -> dict[str, str]:
        return convert_to_pandas_dtypes(table_schema=cls.dagster_table_schema())


def convert_to_pandas_dtypes(table_schema: TableSchema):
    type_mapping = {
        "bigint": "int64",
        "double": "float64",
        "text": "object",
        "timestamp": "datetime64[ns]",
    }

    pandas_dtypes = {}
    for column in table_schema.columns:
        col_name = column.name
        col_type = column.type.lower()
        pandas_dtype = type_mapping.get(col_type, "object")  # Default to object if unknown
        pandas_dtypes[col_name] = pandas_dtype

    return pandas_dtypes
