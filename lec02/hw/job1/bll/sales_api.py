from job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> tuple[bool, str]:
    """
    Fetch sales data from API and save to local disk.
    """
    try:
        sales_data = sales_api.get_sales(date)
        if not sales_data:
            msg = "No sales data retrieved from API"
            print(msg)
            return False, msg
        local_disk.save_to_disk(sales_data, raw_dir, date)
        return True, "success"
    except Exception as e:
        print(f"Error: {e}")
        return False, str(e)
