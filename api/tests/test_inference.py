import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from cron_jobs.run_inference import run_quant_model


class FakeLightGBM:
    def __init__(self):
        self.classes_ = np.array([-1, 1])

    def predict_proba(self, X):
        return np.array([[0.1, 0.9]])


FAKE_CONFIG = """
{
    "buy_threshold": 0.60,
    "sell_threshold": 0.60,
    "req_features": ["feature_1", "feature_2"]
}
"""


@patch("cron_jobs.run_inference.joblib.load")
@patch("builtins.open", new_callable=mock_open, read_data=FAKE_CONFIG)
@patch("cron_jobs.run_inference.shap.TreeExplainer")
def test_run_quant_model_happy_path(mock_explainer, mock_file, mock_joblib):
    """Test that run_quant_model returns a valid BUY signal with expected fields.

    This simulates a happy path where the model predicts a high probability for the
    positive class and SHAP returns feature contributions. The test asserts the
    presence and types of key output fields.
    """

    mock_joblib.return_value = FakeLightGBM()

    fake_explainer_instance = MagicMock()

    fake_explainer_instance.shap_values.return_value = [
        np.array([[-0.2, -0.4]]),
        np.array([[0.2, 0.8]]),
    ]
    mock_explainer.return_value = fake_explainer_instance

    daily_data = [
        {
            "ticker": "AAPL",
            "date": "2026-07-09",
            "close": 150.0,
            "feature_1": 1.5,
            "feature_2": 3.2,
        }
    ]

    signals = run_quant_model(daily_data)

    assert len(signals) == 1
    signal = signals[0]

    assert signal["ticker"] == "AAPL"
    assert signal["ml_decision"] == "BUY"
    assert signal["conviction_zone"] == "HOT"
    assert signal["probability"] == 0.9
    assert isinstance(signal["top_drivers"], list)


@patch("cron_jobs.run_inference.joblib.load")
@patch("builtins.open", new_callable=mock_open, read_data=FAKE_CONFIG)
@patch("cron_jobs.run_inference.shap.TreeExplainer")
def test_run_quant_model_critical_data_hole(mock_explainer, mock_file, mock_joblib):
    """Test that run_quant_model raises for missing or NaN required features.

    If required features contain NaN or are missing for a ticker, the function
    should raise a ValueError indicating a critical data hole for that ticker.
    """

    mock_joblib.return_value = FakeLightGBM()

    daily_data = [
        {
            "ticker": "AAPL",
            "date": "2026-07-09",
            "close": 150.0,
            "feature_1": 1.5,
            "feature_2": np.nan,
        }
    ]

    with pytest.raises(
        ValueError, match="CRITICAL DATA HOLE: Ticker AAPL has missing or NaN values"
    ):
        run_quant_model(daily_data)
