import requests
from pandas import DataFrame, to_datetime

from ...config import settings


def get_hourly_soil_temp_data(
    start_date: str,
    end_date: str,
    lat: float,
    long: float,
    tz: str = "America%2FChicago",
    temp_unit: str = "fahrenheit",
    temp_var: str = "soil_temperature_7_to_28cm",
) -> DataFrame:
    """Get hourly soil temperature data for a given date range and lat/long.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        lat: Latitude.
        long: Longitude.
        tz: Time zone (default: America%2FChicago).
        temp_unit: Temperature unit (default: fahrenheit).
        temp_var: Temperature variable (default: soil_temperature_7_to_28cm).

    Returns:
        A DataFrame with soil temperature data.
    """
    response = requests.get(
        f"{settings.soil_temperature_api.url}?latitude={lat}&longitude={long}&start_date={start_date}&end_date={end_date}&hourly={temp_var}&timezone={tz}&temperature_unit={temp_unit}"
    )
    complete_temperature_data = response.json()

    # Extract components needed to create the df from json
    dates = complete_temperature_data["hourly"]["time"]
    values = complete_temperature_data["hourly"][f"{temp_var}"]

    temperature_data = DataFrame({"date": dates, "temps": values})
    temperature_data["date"] = to_datetime(temperature_data["date"])
    temperature_data.set_index("date", inplace=True)

    return temperature_data


def create_ma(data: DataFrame, ma_interval: int) -> DataFrame:
    """Adds a moving average column on the temperature data.

    Args:
        data: DataFrame to create moving average for.
        ma_interval: Moving average interval.

    Returns:
        A DataFrame with daily temperature data.
    """

    daily_data = data.resample("D").mean()
    daily_data["MA"] = daily_data.rolling(window=ma_interval).mean()

    return daily_data


def return_start_4r_timing(data, ma_interval=7, target_temperature=50) -> str | None:
    """Returns the start of the 4R timing period by find the first date when the
    MA of temp is less than target temperature.

    Args:
        data: DataFrame with soil temperature data.
        ma_interval: Moving average interval (default: 7).
        target_temperature: Desired sustained temperature (default: 50).

    Returns:
        The start of the 4R timing period.
    """

    daily_data = create_ma(data, ma_interval)
    first_date = daily_data[daily_data.temps < target_temperature].first_valid_index()

    return first_date.strftime("%Y-%m-%d")
