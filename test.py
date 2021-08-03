"""
Testing suite functions for geos-chem-schedule
"""

import subprocess
import json
import os
import stat
import sys
import shutil
import datetime
import calendar
from dateutil.relativedelta import relativedelta
import pytest

from core import *
from utils import *


def test_check_inputs():
    """
    Test check_inputs()
    """
    yes_list = ['yes', 'YES', 'Yes', 'Y', 'y']
    no_list = ['NO', 'no', 'NO', 'No', 'N', 'n']

    queue_priority = {
        "name": "queue_priority",
        "valid_data": [1000, "1000", 1023, -1024, 0, -0],
        "invalid_data": [-2000, "bob", 1024]
    }
    queue_name = {
        "name": "queue_name",
        "valid_data": ["nodes"],
        "invalid_data": ["bob", "run"]
    }
    out_of_hours_string = {
        "name": "out_of_hours_string",
        "valid_data": yes_list + no_list,
        "invalid_data": ["bob"],
        "data_logical": "out_of_hours"
    }
    send_email = {
        "name": "send_email",
        "valid_data": yes_list + no_list,
        "invalid_data": ["bob", 1000],
        "data_logical": "email"
    }
    run_script_string = {
        "name": "run_script_string",
        "valid_data": yes_list + no_list,
        "invalid_data": ["bob"],
        "data_logical": "run_script"
    }
    steps = {
        "name": "step",
        "valid_data": ["month", "week", "day"],
        "invalid_data": ["bob"],
    }

    tests = [queue_priority, queue_name, out_of_hours_string,
             send_email, run_script_string, steps]

    for test in tests:
        for data in test["valid_data"]:
            inputs = GC_Job()
            inputs[test["name"]] = data
            # Confirm the valid data works
            try:
                check_inputs(inputs)
            except:
                print("This should fail but it did not:")
                print("Name = ", str(test["name"]), "\ndata = ", str(data))
                print(inputs)
                raise
            # Confirm it changes the logical if one exists
            if "data_logical" in test:
                if data in yes_list:
                    assert inputs[test["data_logical"]], (
                        "Name=", str(test["name"]), "\ndata=", str(data), "\n",
                        str(inputs))
                if data in no_list:
                    assert not inputs[test["data_logical"]], (
                        "Name=", str(test["name"]), "\ndata=", str(data), "\n",
                        str(inputs))

        for data in test["invalid_data"]:
            inputs = GC_Job()
            inputs[test["name"]] = data
            # Confirm the invalid data fails
            with pytest.raises(Exception):
                try:
                    check_inputs(inputs)
                    print("This should fail but it did not:")
                    print("Name = ", str(test["name"]), "\ndata = ", str(data))
                    print(inputs)

                except Exception:
                    raise
    return


def test_check_inputs_steps():
    """
    Test check_inputs() steps
    """
    inputs = GC_Job()
    for step in ["6month", "month", "week", "day"]:
        inputs.step = step
        check_inputs(inputs)
    with pytest.raises(Exception):
        inputs.step = "bob"
        check_inputs(inputs)
    return


def test_get_start_and_end_dates():
    "Test the retreval of the start date and end date"
    # Make a test file
    with open("input.geos", "w") as input_file:
        input_file.write("Start YYYYMMDD, hhmmss  : 20100102 123456\n")
        input_file.write("End   YYYYMMDD, hhmmss  : 20110102 123456")

    start_time, end_time = get_start_and_end_dates()
    assert start_time == "20100102"
    assert end_time == "20110102"
    # Clean up
    os.remove("input.geos")
    return


def test_list_of_times_to_run():
    """
    Make sure the list of times to run makes sense
    """
    monthly = {"step": "month",
               "start_time": "20070101",
               "end_time": "20080101",
               "expected_output": ["20070101", "20070201", "20070301",
                                   "20070401", "20070501", "20070601",
                                   "20070701", "20070801", "20070901",
                                   "20071001", "20071101", "20071201",
                                   "20080101"]
               }
    leap_year = {"step": "week",
                 "start_time": "20140720",
                 "end_time": "20140831",
                 "expected_output": ["20140720", "20140727", "20140803",
                                     "20140810", "20140817", "20140824",
                                     "20140831"]
                 }
    daily = {"step": "day",
             "start_time": "20000101",
             "end_time": "20000106",
             "expected_output": ["20000101", "20000102", "20000103",
                                 "20000104", "20000105", "20000106"]
             }

    tests = [monthly, leap_year, daily]

    for test in tests:
        inputs = GC_Job()
        inputs["step"] = test["step"]
        times = list_of_times_to_run(test["start_time"],
                                     test["end_time"], inputs)
        assert times == test["expected_output"]

    return


def test_create_new_input_file():
    """
    Test the input file editor works
    """

    test_1 = {
        "start_time": "20130601",
        "end_time": "20130608",
        "input_lines": [
            "Start YYYYMMDD, hhmmss  : 20120101 000000\n",
            "End   YYYYMMDD, hhmmss  : 20120109 000000\n",
            "Read and save CSPEC_FULL: f\n",
            "Schedule output for JAN : 3000000000000000000000000000000\n",
            "Schedule output for JUL : 3000000000000000000000000000000\n",
            "Schedule output for JUN : 300000000000000000000000000000\n",
        ],
        "output_lines": [
            "Start YYYYMMDD, hhmmss  : 20130601 000000\n",
            "End   YYYYMMDD, hhmmss  : 20130608 000000\n",
            "Read and save CSPEC_FULL: T\n",
            "Schedule output for JAN : 0000000000000000000000000000000\n",
            "Schedule output for JUL : 0000000000000000000000000000000\n",
            "Schedule output for JUN : 000000030000000000000000000000\n",
        ],
    }

    tests = [test_1]
    for test in tests:
        testing_lines = create_new_input_file(test["start_time"],
                                              test["end_time"],
                                              test["input_lines"])
        correct_lines = test["output_lines"]
        assert testing_lines == correct_lines

    return


def test_update_output_line():
    """
    Tests for update_output_line
    """
    test_1 = {
        "end_time": "20140305",
        "linein": "Schedule output for MAR : 3000000000000000000000000000000\n",
        "lineout": "Schedule output for MAR : 0000300000000000000000000000000\n",
    }
    test_2 = {
        "end_time": "20140405",
        "linein": "Schedule output for MAR : 3000000000000000000000000000000\n",
        "lineout": "Schedule output for MAR : 0000000000000000000000000000000\n",
    }
    test_3 = {
        "end_time": "20140831",
        "linein": "Schedule output for AUG : 0000000000030000000000000000000\n",
        "lineout": "Schedule output for AUG : 0000000000000000000000000000003\n",
    }
    test_4 = {
        "end_time": "20140831",
        "linein": "Schedule output for APR : 000000000000000000000000000000\n",
        "lineout": "Schedule output for APR : 000000000000000000000000000000\n",
    }
    test_5 = {
        "end_time": "20150630",
        "linein": "Schedule output for JUN : 333333333333333333333333333333\n",
        "lineout": "Schedule output for JUN : 000000000000000000000000000003\n",
    }

    tests = [test_1, test_2, test_3, test_4, test_5]
    for test in tests:
        assert test["lineout"] == update_output_line(
            test["linein"], test["end_time"])

    return


def test_get_variables_from_cli():
    """
    Test that variables passed from the cli make it into the class.
    """
    #########
    # To-do
    ########
    #
    # Write this test
    #
    ##########
    return


def test_get_arguments():
    """
    Test that the passed arguments get assigned to the class.
    """
    ########
    # TO DO
    ########
    #
    # Write these tests ...
    #
    ########
    return
