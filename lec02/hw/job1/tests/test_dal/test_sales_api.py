"""
Tests sales_api.py module.
"""

from unittest import TestCase, mock

from job1.dal.sales_api import fetch_data, get_sales


class GetSalesTestCase(TestCase):
    """
    Test sales_api.get_sales function.
    """

    @mock.patch("job1.dal.sales_api.fetch_data")
    def test_get_sales_single_page(self, fetch_data_mock: mock.MagicMock):
        """
        Test get_sales with single page of data
        """
        fake_data = [
            {"id": 1, "product": "Product A", "amount": 100},
            {"id": 2, "product": "Product B", "amount": 200},
        ]
        fetch_data_mock.side_effect = [fake_data, None]

        result = get_sales("2022-08-09")

        self.assertEqual(result, fake_data)
        self.assertEqual(fetch_data_mock.call_count, 2)
        fetch_data_mock.assert_any_call("2022-08-09", 1)
        fetch_data_mock.assert_any_call("2022-08-09", 2)

    @mock.patch("job1.dal.sales_api.fetch_data")
    def test_get_sales_multiple_pages(self, fetch_data_mock: mock.MagicMock):
        """
        Test get_sales with multiple pages of data
        """
        page1_data = [{"id": 1, "product": "Product A", "amount": 100}]
        page2_data = [{"id": 2, "product": "Product B", "amount": 200}]
        page3_data = [{"id": 3, "product": "Product C", "amount": 300}]

        fetch_data_mock.side_effect = [page1_data, page2_data, page3_data, None]

        result = get_sales("2022-08-09")

        expected_data = page1_data + page2_data + page3_data
        self.assertEqual(result, expected_data)
        self.assertEqual(fetch_data_mock.call_count, 4)

    @mock.patch("job1.dal.sales_api.fetch_data")
    def test_get_sales_no_data(self, fetch_data_mock: mock.MagicMock):
        """
        Test get_sales when API returns no data
        """
        fetch_data_mock.return_value = None

        result = get_sales("2022-08-09")

        self.assertEqual(result, [])
        fetch_data_mock.assert_called_once_with("2022-08-09", 1)

    @mock.patch("job1.dal.sales_api.fetch_data")
    def test_get_sales_empty_page(self, fetch_data_mock: mock.MagicMock):
        """
        Test get_sales when API returns empty list
        """
        fetch_data_mock.return_value = []

        result = get_sales("2022-08-09")

        self.assertEqual(result, [])
        fetch_data_mock.assert_called_once_with("2022-08-09", 1)


class FetchDataTestCase(TestCase):
    """
    Test sales_api.fetch_data function.
    """

    @mock.patch("job1.dal.sales_api.requests.get")
    def test_fetch_data_success(self, requests_get_mock: mock.MagicMock):
        """
        Test fetch_data with successful API response
        """
        fake_response_data = [{"id": 1, "product": "Product A", "amount": 100}]
        requests_get_mock.return_value.status_code = 200
        requests_get_mock.return_value.json.return_value = fake_response_data

        result = fetch_data("2022-08-09", 1)

        self.assertEqual(result, fake_response_data)
        requests_get_mock.assert_called_once()

    @mock.patch("job1.dal.sales_api.requests.get")
    def test_fetch_data_not_found(self, requests_get_mock: mock.MagicMock):
        """
        Test fetch_data when API returns 404
        """
        requests_get_mock.return_value.status_code = 404

        result = fetch_data("2022-08-09", 1)

        self.assertIsNone(result)
        requests_get_mock.assert_called_once()

    @mock.patch("job1.dal.sales_api.requests.get")
    def test_fetch_data_server_error(self, requests_get_mock: mock.MagicMock):
        """
        Test fetch_data when API returns 500
        """
        requests_get_mock.return_value.status_code = 500

        result = fetch_data("2022-08-09", 1)

        self.assertIsNone(result)
        requests_get_mock.assert_called_once()

    @mock.patch("job1.dal.sales_api.requests.get")
    @mock.patch("job1.dal.sales_api.AUTH_TOKEN", "test-token-123")
    def test_fetch_data_uses_auth_token(self, requests_get_mock: mock.MagicMock):
        """
        Test that fetch_data uses AUTH_TOKEN in headers
        """
        requests_get_mock.return_value.status_code = 200
        requests_get_mock.return_value.json.return_value = []

        fetch_data("2022-08-09", 1)

        call_args = requests_get_mock.call_args
        self.assertIn("headers", call_args.kwargs)
        self.assertEqual(call_args.kwargs["headers"]["Authorization"], "test-token-123")
