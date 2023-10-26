# # import flask_sqlalchemy
import pandas as pd
import base64
import sys
import pytest
import os

sys.path.append("src")
from apps import dataLoad


# parameter setup
lme_vols = [
    "TodaysVolatility_09May23.csv",
    "TodaysVolatility_12May23.csv",
    "TodaysVolatility_18Aug23.csv",
    "TodaysVolatility_18Oct23.csv",
    "TodaysVolatility_19May23.csv",
    "TodaysVolatility_19Oct23.csv",
]


# create fixture for test data
@pytest.fixture(scope="module")
def test_lme_vols():
    data = {}
    # Directory where your CSV files are located
    csv_directory = "tests/test_assets"

    # List the CSV files in the directory
    csv_files = [
        file for file in os.listdir(csv_directory) if file.startswith("Todays")
    ]

    for file in csv_files:
        file_path = os.path.join(csv_directory, file)
        df = pd.read_csv(file_path)
        data[file] = df

    return data


# test parse_data function
@pytest.mark.parametrize(
    "file, expected_type",
    [
        ("TodaysVolatility_09May23.csv", pd.DataFrame),
        ("TodaysVolatility_12May23.csv", pd.DataFrame),
        ("TodaysVolatility_18Aug23.csv", pd.DataFrame),
        ("TodaysVolatility_18Oct23.csv", pd.DataFrame),
        ("TodaysVolatility_19May23.csv", pd.DataFrame),
        ("TodaysVolatility_19Oct23.csv", pd.DataFrame),
    ],
)
def test_parse_data(file, expected_type):
    path = "tests/test_assets/" + file

    encoded_data = base64.b64encode(open(path, "rb").read()).decode("utf-8")
    uploaded_file = f"data:application/vnd.ms-excel;base64,{encoded_data}"

    file_df = dataLoad.parse_data(uploaded_file, "sample.csv")
    assert isinstance(file_df, pd.DataFrame)
    assert file_df.shape[0] > 0
    assert file_df.shape[1] > 0


# # functions left to test in the rec files:


@pytest.mark.parametrize(
    "data_frame, expected_answer",
    [
        ("TodaysVolatility_09May23.csv", (0, "Vols uploaded successfully")),  # normal
        (
            "TodaysVolatility_12May23.csv",
            (1, "Date column not formatted correctly"),
        ),
        ("TodaysVolatility_18Aug23.csv", (1, "Multiple dates in file")),
        ("TodaysVolatility_18Oct23.csv", (1, "Series column not formatted correctly")),
        (
            "TodaysVolatility_19May23.csv",
            (2, "Vols appear to be in Absolute format, not Relative"),
        ),
        ("TodaysVolatility_19Oct23.csv", (0, "Vols uploaded successfully")),
    ],
)
def test_data_frames_with_expected_answer(data_frame, expected_answer, test_lme_vols):
    # Access the data frame from the fixture using the filename as a key
    data = test_lme_vols[data_frame]

    # Perform tests using the data frame and compare with the expected answer
    result = dataLoad.validate_lme_vols(data)  # Replace with the actual function
    assert result == expected_answer
