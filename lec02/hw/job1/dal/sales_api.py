from typing import Any, Dict, List

import requests
from job1.config import AUTH_TOKEN

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    print(f"Starting data fetch for date: {date}")
    full_data: List[Dict[str, Any]] = []
    page = 1
    while True:
        records = fetch_data(date, page)
        if not records:
            break
        full_data.extend(records)
        page += 1
    return full_data


def fetch_data(date: str, page: int) -> list[dict] | None:
    """
    Fetch a single page of data from the API.

    :param date: date to filter data
    :param page: page number to fetch
    :return: list of records
    """
    endpoint = f"{API_URL}/sales?date={date}&page={page}"
    headers = {"Authorization": AUTH_TOKEN}
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        print(f"API request not found data, status code {response.status_code}")
        return
    else:
        print(f"Fetched page {page} for date {date}")
        return response.json()
