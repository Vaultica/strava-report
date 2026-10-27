#!/usr/bin/env python3

import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import get_strava_activities as app


class TestStravaPipeline(unittest.TestCase):

  # --- CLI Argument Parsing Tests ---
  def test_parse_arguments_default(self):
    with patch("sys.argv", ["get_strava_activities.py"]):
      args = app.parse_arguments()
      self.assertEqual(args.num_activities, 30)
      self.assertIsNone(args.activity_id)

  def test_parse_arguments_custom_count(self):
    with patch("sys.argv", ["get_strava_activities.py", "-n", "100"]):
      args = app.parse_arguments()
      self.assertEqual(args.num_activities, 100)
      self.assertIsNone(args.activity_id)

  def test_parse_arguments_activity_id(self):
    with patch(
        "sys.argv", ["get_strava_activities.py", "-a", "18455621907"]
    ):
      args = app.parse_arguments()
      self.assertEqual(args.activity_id, 18455621907)

  # --- Token & Secret Manager Tests ---
  @patch("get_strava_activities.get_secret")
  @patch("time.time", return_value=1000)
  def test_get_valid_token_unexpired(self, mock_time, mock_get_secret):
    mock_get_secret.side_effect = lambda secret_id: {
        "strava_credentials": {
            "client_id": "123",
            "client_secret": "abc",
        },
        "strava_token_data": {
            "access_token": "valid_token",
            "refresh_token": "ref_token",
            "expires_at": 2000,  # Unexpired
        },
    }[secret_id]

    token = app.get_valid_token()
    self.assertEqual(token, "valid_token")

  @patch("get_strava_activities.save_secret_version")
  @patch("get_strava_activities.get_secret")
  @patch("get_strava_activities.Client")
  @patch("time.time", return_value=3000)
  def test_get_valid_token_expired_refreshes(
      self, mock_time, mock_client_cls, mock_get_secret, mock_save_secret
  ):
    mock_get_secret.side_effect = lambda secret_id: {
        "strava_credentials": {
            "client_id": "123",
            "client_secret": "abc",
        },
        "strava_token_data": {
            "access_token": "expired_token",
            "refresh_token": "old_refresh",
            "expires_at": 2000,  # Expired
        },
    }[secret_id]

    mock_client_inst = MagicMock()
    mock_client_inst.refresh_access_token.return_value = {
        "access_token": "new_refreshed_token",
        "refresh_token": "new_refresh",
        "expires_at": 4000,
    }
    mock_client_cls.return_value = mock_client_inst

    token = app.get_valid_token()

    self.assertEqual(token, "new_refreshed_token")
    mock_client_inst.refresh_access_token.assert_called_once_with(
        client_id="123", client_secret="abc", refresh_token="old_refresh"
    )
    mock_save_secret.assert_called_once()

  # --- Activity Extraction Tests ---
  @patch("requests.get")
  def test_fetch_single_activity(self, mock_requests_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"id": 18455621907, "name": "Morning Run"}
    mock_requests_get.return_value = mock_resp

    res = app.fetch_activities("dummy_token", activity_id=18455621907)
    self.assertEqual(len(res), 1)
    self.assertEqual(res[0]["id"], 18455621907)

  @patch("requests.get")
  def test_fetch_activities_pagination(self, mock_requests_get):
    mock_resp_p1 = MagicMock()
    mock_resp_p1.json.return_value = [{"id": i} for i in range(200)]

    mock_resp_p2 = MagicMock()
    mock_resp_p2.json.return_value = [{"id": i} for i in range(200, 250)]

    mock_requests_get.side_effect = [mock_resp_p1, mock_resp_p2]

    res = app.fetch_activities("dummy_token", num_activities=250)
    self.assertEqual(len(res), 250)
    self.assertEqual(mock_requests_get.call_count, 2)

  # --- Data Transformation Tests ---
  def test_transform_activities_schema_and_coordinates(self):
    raw_activities = [{
        "id": 1,
        "name": "Evening Ride",
        "start_latlng": [51.5074, -0.1278],
        "distance": 15000.5,
        "extra_field": "ignore_me",
    }]
    selected_cols = ["id", "name", "start_latlng", "distance", "gear_id"]

    df = app.transform_activities(raw_activities, selected_cols)
    self.assertIsInstance(df, pd.DataFrame)
    self.assertEqual(list(df.columns), selected_cols)
    self.assertEqual(df.iloc[0]["start_latlng"], "51.5074, -0.1278")
    self.assertIsNone(df.iloc[0]["gear_id"])
    self.assertNotIn("extra_field", df.columns)

  def test_transform_activities_empty_latlng(self):
    raw_activities = [{"id": 2, "name": "Virtual Run", "start_latlng": []}]
    df = app.transform_activities(raw_activities, ["id", "start_latlng"])
    self.assertIsNone(df.iloc[0]["start_latlng"])

  # --- End-to-End Main Execution Flow ---
  @patch("get_strava_activities.load_to_bigquery")
  @patch("get_strava_activities.transform_activities")
  @patch("get_strava_activities.fetch_activities")
  @patch("get_strava_activities.get_valid_token")
  @patch("get_strava_activities.parse_arguments")
  def test_main_pipeline_flow(
      self,
      mock_parse,
      mock_token,
      mock_fetch,
      mock_transform,
      mock_load,
  ):
    mock_parse.return_value = MagicMock(num_activities=30, activity_id=None)
    mock_token.return_value = "fake_token"
    mock_fetch.return_value = [{"id": 100}]
    mock_df = MagicMock()
    mock_transform.return_value = mock_df

    app.main()

    mock_parse.assert_called_once()
    mock_token.assert_called_once()
    mock_fetch.assert_called_once_with(
        "fake_token", num_activities=30, activity_id=None
    )
    mock_transform.assert_called_once_with([{"id": 100}], app.MY_COLS)
    mock_load.assert_called_once_with(mock_df)


if __name__ == "__main__":
  unittest.main()
