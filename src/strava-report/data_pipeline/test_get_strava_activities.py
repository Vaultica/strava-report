#!/usr/bin/env python3

import unittest
from unittest.mock import MagicMock, call, mock_open, patch

import pandas as pd

# Import functions from your refactored module
import get_strava_activities as app


class TestStravaPipeline(unittest.TestCase):

  # -------------------------------------------------------------------------
  # 1. CLI Parsing Tests
  # -------------------------------------------------------------------------
  def test_parse_arguments_default(self):
    """Test defaults when no flags are supplied."""
    with patch("sys.argv", ["get_strava_activities_2.py"]):
      args = app.parse_arguments()
      self.assertEqual(args.num_activities, 30)
      self.assertIsNone(args.activity_id)

  def test_parse_arguments_custom_count(self):
    """Test passing -n / --num-activities."""
    with patch("sys.argv", ["get_strava_activities_2.py", "-n", "75"]):
      args = app.parse_arguments()
      self.assertEqual(args.num_activities, 75)
      self.assertIsNone(args.activity_id)

  def test_parse_arguments_activity_id(self):
    """Test passing -a / --activity-id."""
    with patch(
        "sys.argv", ["get_strava_activities_2.py", "-a", "18455621907"]
    ):
      args = app.parse_arguments()
      self.assertEqual(args.activity_id, 18455621907)

  # -------------------------------------------------------------------------
  # 2. Authentication & Token Tests
  # -------------------------------------------------------------------------
  @patch("time.time", return_value=1000)
  @patch(
      "pickle.load",
      return_value={"access_token": "valid_token", "expires_at": 2000},
  )
  @patch("builtins.open", new_callable=mock_open)
  def test_get_valid_token_unexpired(
      self, mock_file, mock_pickle_load, mock_time
  ):
    """Token should be used directly when not expired."""
    token = app.get_valid_token("dummy.pickle", "client_id", "client_secret")
    self.assertEqual(token, "valid_token")

  @patch("time.time", return_value=3000)
  @patch(
      "pickle.load",
      return_value={
          "access_token": "old_token",
          "refresh_token": "ref_123",
          "expires_at": 2000,
      },
  )
  @patch("pickle.dump")
  @patch("builtins.open", new_callable=mock_open)
  @patch("get_strava_activities.Client")
  def test_get_valid_token_expired_refreshes(
      self, mock_client_cls, mock_file, mock_pickle_dump, mock_pickle_load, mock_time
  ):
    """Token should refresh and save back to disk when expired."""
    mock_client = MagicMock()
    mock_client.refresh_access_token.return_value = {
        "access_token": "new_refreshed_token",
        "refresh_token": "new_ref_123",
        "expires_at": 5000,
    }
    mock_client_cls.return_value = mock_client

    token = app.get_valid_token("dummy.pickle", "client_id", "client_secret")

    self.assertEqual(token, "new_refreshed_token")
    mock_client.refresh_access_token.assert_called_once_with(
        client_id="client_id",
        client_secret="client_secret",
        refresh_token="ref_123",
    )
    mock_pickle_dump.assert_called_once()

  # -------------------------------------------------------------------------
  # 3. API Fetching Tests
  # -------------------------------------------------------------------------
  @patch("requests.get")
  def test_fetch_single_activity(self, mock_get):
    """Test fetching a specific activity by ID."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"id": 18455621907, "name": "Morning Ride"}
    mock_get.return_value = mock_resp

    result = app.fetch_activities("fake_token", activity_id=18455621907)

    self.assertEqual(len(result), 1)
    self.assertEqual(result[0]["id"], 18455621907)
    mock_get.assert_called_once_with(
        "https://www.strava.com/api/v3/activities/18455621907",
        headers={"Authorization": "Bearer fake_token"},
    )

  @patch("requests.get")
  def test_fetch_activities_pagination(self, mock_get):
    """Test pagination math when requesting > 200 activities (e.g. 250)."""
    # 250 total: Page 1 (200), Page 2 (50)
    mock_resp_p1 = MagicMock()
    mock_resp_p1.json.return_value = [{"id": i} for i in range(200)]
    mock_resp_p2 = MagicMock()
    mock_resp_p2.json.return_value = [{"id": i} for i in range(200, 250)]

    mock_get.side_effect = [mock_resp_p1, mock_resp_p2]

    result = app.fetch_activities("fake_token", num_activities=250)

    self.assertEqual(len(result), 250)
    self.assertEqual(mock_get.call_count, 2)

    # Check page query params
    expected_calls = [
        call(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": "Bearer fake_token"},
            params={"page": 1, "per_page": 200},
        ),
        call(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": "Bearer fake_token"},
            params={"page": 2, "per_page": 50},
        ),
    ]
    mock_get.assert_has_calls(expected_calls)

  # -------------------------------------------------------------------------
  # 4. Data Transformation Tests
  # -------------------------------------------------------------------------
  def test_transform_activities_schema_and_coordinates(self):
    """Test coordinate formatting, column pruning, and missing keys."""
    raw_api_payload = [{
        "id": 18455621907,
        "name": "Morning Ride",
        "start_latlng": [45.123, 7.456],  # Array coordinate format
        "average_speed": 6.2,
        "unexpected_extra_field": "drop_me",
    }]

    selected_cols = ["id", "name", "start_latlng", "average_speed", "gear_id"]
    df = app.transform_activities(raw_api_payload, selected_cols)

    self.assertIsInstance(df, pd.DataFrame)
    self.assertEqual(list(df.columns), selected_cols)
    # Coordinate list must be converted into string format
    self.assertEqual(df.loc[0, "start_latlng"], "45.123, 7.456")
    # Missing fields should default to None
    self.assertIsNone(df.loc[0, "gear_id"])
    # Extra payload fields must be excluded
    self.assertNotIn("unexpected_extra_field", df.columns)

  def test_transform_activities_empty_latlng(self):
    """Test handling of null or empty coordinates."""
    raw_api_payload = [{"id": 1, "start_latlng": []}]
    df = app.transform_activities(raw_api_payload, ["id", "start_latlng"])
    self.assertIsNone(df.loc[0, "start_latlng"])

  # -------------------------------------------------------------------------
  # 5. Full Pipeline End-to-End Orchestration Test
  # -------------------------------------------------------------------------
  @patch("get_strava_activities.load_to_bigquery")
  @patch("get_strava_activities.save_local_backup")
  @patch("get_strava_activities.fetch_activities")
  @patch("get_strava_activities.get_valid_token")
  @patch("get_strava_activities.parse_arguments")
  def test_main_pipeline_flow(
      self,
      mock_args,
      mock_token,
      mock_fetch,
      mock_backup,
      mock_load_bq,
  ):
    """Verify that main() calls each step in order with correct parameters."""
    mock_args.return_value = MagicMock(num_activities=10, activity_id=None)
    mock_token.return_value = "mock_access_token"
    mock_fetch.return_value = [
        {"id": 1, "name": "Ride 1", "start_latlng": [1.0, 2.0]}
    ]

    app.main()

    mock_token.assert_called_once()
    mock_fetch.assert_called_once_with(
        "mock_access_token", num_activities=10, activity_id=None
    )
    mock_backup.assert_called_once()
    mock_load_bq.assert_called_once()


if __name__ == "__main__":
  unittest.main()
