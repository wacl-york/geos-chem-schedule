"""
Utility functions for geos-chem-schedule
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


def clear_screen():
    """
    Clear the screen of the terminal for the UI
    """
    os.system('cls' if os.name == 'nt' else 'clear')
    return


def get_start_and_end_dates():
    """
    Get the start date and end date from input.geos
    """
    input_geos = open('input.geos', 'r')
    for line in input_geos:
        if line.startswith("Start YYYYMMDD"):
            start_date = line[26:34]
        if line.startswith("End   YYYYMMDD"):
            end_date = line[26:34]

    # Error checking though print...
    print("Start time = {start_date}".format(start_date=start_date))
    print("End time = {end_date}".format(end_date=end_date))
    input_geos.close()

    return start_date, end_date


def backup_the_input_files(inputs=None):
    """
    Save a copy of the original input file
    """
    input_files = ["input.geos"]
    if inputs.manage_hemco_files:
        input_files += ['HEMCO_Config.rc']
    for input_file in input_files:
        backup_input_file = '{}.orig'.format(input_file)
        if not os.path.isfile(backup_input_file):
            shutil.copyfile(input_file, backup_input_file)
    return


def setup_script():
    """
    Creates a symbolic link to allow running "geos-chem-schedule" from any directory
    """
    print("\n",
          "geos-chem-schedule setup complete. Change your default settings in settings.json\n",
          "To run the script from anywhere with the geos-chem-schedule command,",
          "copy the following code into your terminal. \n")

    script_location = os.path.realpath(__file__)
    # make sure the script is excecutable
    print("chmod 755 {script}".format(script=script_location))
    # Make sure there is a ~/bin file
    print("mkdir -p $HOME/bin")
    # Create a symlink from the file to the bin
    print("ln -s {script} $HOME/bin/geos-chem-schedule".format(script=script_location))
    # Make sure the ~/bin is in the bashrc
    # with open('$HOME/.bashrc','a') as bashrc:
    #        bashrc.write('## Written by geos-chem-schedule')
    #        bashrc.write('export PATH=$PATH:$HOME/bin')
    print('echo "## Written by geos-chem-schedule " >> $HOME/.bashrc')
    print('echo "export PATH=\$PATH:\$HOME/bin" >> $HOME/.bashrc')
    # Source the bashrc
    print("source $HOME/.bashrc")
    print("\n")
    sys.exit()


def is_current_year_a_leap_year(year):
    """
    Check if current year is a leap year

    Parameters
    -------
    year (int): year to check if it is a leap year

    Returns
    -------
    (bool)
    """
    import calender
    return calendar.isleap(year)