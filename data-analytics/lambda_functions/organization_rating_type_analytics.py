
import json
import os
from datetime import date, datetime, timedelta

import pandas as pd

try:
    import psycopg2
except ImportError:
    psycopg2 = None


SCHEMA = "virginia_dev_saayam_rdbms"


def load_data():
    """Load organization, state, and country data from CSVs or PostgreSQL."""
    use_mock = os.getenv("USE_MOCK_DATA", "true").strip().lower() == "true"

    if use_mock:
        data_dir = os.getenv(
            "MOCK_DATA_DIR",
            os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "sql")
            ),
        )

        def read_csv_file(filename):
            path = os.path.join(data_dir, filename)
            if not os.path.isfile(path):
                return None
            return pd.read_csv(path)

        organizations = read_csv_file("organizations.csv")
        states = read_csv_file("states.csv")
        countries = read_csv_file("countries.csv")

        if organizations is None:
            raise ValueError(
                f"organizations.csv was not found in MOCK_DATA_DIR: {data_dir}"
            )

        return organizations, states, countries

    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is required when USE_MOCK_DATA is false."
        )

    connection = psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=int(os.getenv("DB_PORT", "5432")),
        connect_timeout=10,
    )

    try:
        organizations = pd.read_sql_query(
            f'SELECT * FROM "{SCHEMA}".organizations', connection
        )
        states = pd.read_sql_query(
            f'SELECT * FROM "{SCHEMA}".states', connection
        )
        countries = pd.read_sql_query(
            f'SELECT * FROM "{SCHEMA}".countries', connection
        )
    finally:
        connection.close()

    return organizations, states, countries


def get_column(dataframe, candidates, description):
    """Find a column without depending on capitalization."""
    columns = {str(column).lower(): column for column in dataframe.columns}

    for candidate in candidates:
        if candidate.lower() in columns:
            return columns[candidate.lower()]

    raise ValueError(
        f"Could not find the {description} column. "
        f"Expected one of: {', '.join(candidates)}"
    )


def prepare_data(organizations, states, countries, country):
    """Normalize columns and apply the optional country filter."""
    organizations = organizations.copy()



    rating_column = get_column(
        organizations, ["org_rating", "rating"], "organization rating"
    )
    date_column = get_column(
        organizations,
        ["created_at", "registered_at", "created_date"],
        "organization creation date",
    )
    type_column = get_column(organizations, ["org_type"], "organization type")

    organizations["_rating"] = pd.to_numeric(
        organizations[rating_column], errors="coerce"
    )
    organizations["_created"] = pd.to_datetime(
        organizations[date_column], errors="coerce", utc=True
    ).dt.date
    organizations["_type"] = (
        organizations[type_column].astype("string").str.strip().str.lower()
    )

    # Standardize organization type labels for the response.
    organizations["_type"] = organizations["_type"].replace(
        {
            "nonprofit": "non_profit",
            "non-profit": "non_profit",
            "non profit": "non_profit",
            "not-for-profit": "non_profit",
            "not for profit": "non_profit",
            "for-profit": "for_profit",
            "for profit": "for_profit",
            "forprofit": "for_profit",
        }
    )

    organizations = organizations.dropna(subset=["_created"])

    if country and country.strip().upper() != "ALL":
        if states is None or countries is None:
            raise ValueError(
                "Country filtering requires states.csv and countries.csv "
                "in MOCK_DATA_DIR."
            )

        state_id = get_column(organizations, ["state_id"], "organization state ID")
        states_state_id = get_column(states, ["state_id"], "state ID")
        states_country_id = get_column(states, ["country_id"], "state country ID")
        countries_country_id = get_column(
            countries, ["country_id"], "country ID"
        )
        country_code = get_column(
            countries, ["country_code"], "country code"
        )

        state_map = states[[states_state_id, states_country_id]].copy()
        country_map = countries[[countries_country_id, country_code]].copy()

        joined = organizations.merge(
            state_map,
            left_on=state_id,
            right_on=states_state_id,
            how="left",
        )
        joined = joined.merge(
            country_map,
            left_on=states_country_id,
            right_on=countries_country_id,
            how="left",
        )

        joined = joined[
            joined[country_code].astype("string").str.upper()
            == country.strip().upper()
        ]
        organizations = joined

    return organizations


def parse_date(value, parameter_name):
    """Parse an ISO date and return a date object."""
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(
            f"{parameter_name} must be a valid date in YYYY-MM-DD format."
        )


def get_date_pair(params, start_name, end_name):
    """Return a validated date pair, or None when neither is supplied."""
    start_value = params.get(start_name)
    end_value = params.get(end_name)

    start_supplied = start_value is not None and str(start_value).strip() != ""
    end_supplied = end_value is not None and str(end_value).strip() != ""

    if not start_supplied and not end_supplied:
        return None

    if not start_supplied or not end_supplied:
        raise ValueError(
            f"Provide both {start_name} and {end_name}."
        )

    start_date = parse_date(start_value, start_name)
    end_date = parse_date(end_value, end_name)

    if start_date > end_date:
        raise ValueError(f"{start_name} cannot be later than {end_name}.")

    return start_date, end_date


def filter_dates(data, start_date, end_date):
    """Filter records inclusively by creation date."""
    return data[
        data["_created"].between(start_date, end_date, inclusive="both")
    ].copy()


def build_rating_distribution(data):
    """Count only ratings present in the selected date range."""
    ratings = data[data["_rating"].isin([1, 2, 3, 4, 5])]

    counts = ratings.groupby("_rating").size().sort_index()

    return [
        {"rating": int(rating), "count": int(count)}
        for rating, count in counts.items()
    ]


def build_organization_mix_trend(data, start_date, end_date, monthly=False):
    """Build sparse cumulative totals separately for each organization type."""
    data = filter_dates(data, start_date, end_date)

    if monthly:
        data["_period"] = pd.to_datetime(
            data["_created"].astype(str)
        ).dt.strftime("%Y-%m")
    else:
        data["_period"] = data["_created"].apply(lambda value: value.isoformat())

    result = {"non_profit": [], "for_profit": []}

    for organization_type in ("non_profit", "for_profit"):
        type_data = data[data["_type"] == organization_type]

        if type_data.empty:
            continue

        counts = type_data.groupby("_period").size().sort_index()
        cumulative = counts.cumsum()

        result[organization_type] = [
            {"date": str(period), "count": int(total)}
            for period, total in cumulative.items()
        ]

    return result


def build_bucket(data, start_date, end_date, monthly=False):
    """Build both charts for a standard date range."""
    window_data = filter_dates(data, start_date, end_date)

    return {
        "rating_distribution": build_rating_distribution(window_data),
        "organization_mix_trend": build_organization_mix_trend(
            data, start_date, end_date, monthly=monthly
        ),
    }


def build_response(params, organizations, states, countries):
    country = str(params.get("country", "ALL")).strip() or "ALL"

    data = prepare_data(organizations, states, countries, country)
    today = date.today()

    rating_dates = get_date_pair(
        params, "rating_start_date", "rating_end_date"
    )
    type_dates = get_date_pair(
        params, "type_start_date", "type_end_date"
    )

    # If either custom date pair is supplied, return only Custom.
    if rating_dates is not None or type_dates is not None:
        custom = {}

        if rating_dates is not None:
            start_date, end_date = rating_dates
            rating_data = filter_dates(data, start_date, end_date)
            custom["rating_distribution"] = build_rating_distribution(
                rating_data
            )

        if type_dates is not None:
            start_date, end_date = type_dates
            custom["organization_mix_trend"] = (
                build_organization_mix_trend(data, start_date, end_date)
            )

        return {"Custom": custom}

    seven_day_start = today - timedelta(days=6)
    thirty_day_start = today - timedelta(days=29)
    year_start = today - timedelta(days=364)

    return {
        "7D": build_bucket(data, seven_day_start, today),
        "30D": build_bucket(data, thirty_day_start, today),
        "1Y": build_bucket(data, year_start, today, monthly=True),
        "All": build_bucket(
            data,
            min(data["_created"]) if not data.empty else today,
            today,
            monthly=True,
        ),
        "Custom": {},
    }


def lambda_handler(event, context=None):
    """AWS Lambda entry point."""
    try:
        params = event.get("queryStringParameters") or {}

        # Also allow direct parameter dictionaries during local testing.
        if not params and isinstance(event, dict):
            params = event

        organizations, states, countries = load_data()
        response = build_response(
            params, organizations, states, countries
        )

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response),
        }

    except ValueError as exc:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(exc)}),
        }

    except Exception as exc:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(exc)}),
        }


if __name__ == "__main__":
    result = lambda_handler({"country": "ALL"})
    print(json.dumps(json.loads(result["body"]), indent=2))
