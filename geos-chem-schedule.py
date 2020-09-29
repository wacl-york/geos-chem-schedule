#!/usr/bin/env python
"""
A script to split up GEOS-Chem jobs and submit via a job scheduler

Notes
-------
 - Jobs can be split by day, week, month, or numbers of months etc...
 - This allows simple management of job submission
 - Large jobs can also be fitted in queues with lower time limits
 - fairer access
 - You can either pass as arguments or run a UI if no arguments are passed.
 - The jobs can call the next job in the sequence meaning you can submit in the same way.
 - see "$ python geos-chem-schedule.py --help" for more information.
 - TODO:
    - Make the stop after the final job in a set end in a cleaner way
    - Alter SLURM script to stop it seeing BASH variables as commands
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

# Import the functions called here for now...
from core import GC_Job, get_arguments, check_inputs, get_start_and_end_dates, list_of_times_to_run, backup_the_input_file, create_the_input_files, run_job_script
from core import create_PBS_queue_files, create_PBS_run_script
from core import create_SLURM_queue_files, create_SLURM_run_script, create_SLURM_run_script2submit_together

# Master debug switch for the main driver
DEBUG = False


def main(debug=DEBUG):
    """
    Run the main driver of the 'geos-chem-schedule' module
    """
    # Get the default inputs as a class
    inputs = GC_Job()

    # Get the arguments from the command line or UI
    inputs = get_arguments(inputs, debug=DEBUG)

    # Check all the inputs are valid
    inputs = check_inputs(inputs, debug=DEBUG)

    # Check the start and end dates are compatible with the script
    start_date, end_date = get_start_and_end_dates()

    # Calculate the list of times to run the model for
    times = list_of_times_to_run(start_date, end_date, inputs)

    # Make a backup of the input.geos file
    backup_the_input_file()

    # Create the individual time input files
    create_the_input_files(times, inputs=inputs)

    # Create the files required by the specific scheduler
    if inputs.scheduler == 'PBS':
        # Create the PBS queue files
        create_PBS_queue_files(times, inputs=inputs, debug=DEBUG)
        # Create the PSB run script
        create_PBS_run_script(time)
        # Send the script to the queue if requested
        run_job_script(inputs.run_script, filename="run_geos_PBS.sh")
    elif (inputs.scheduler == 'SLURM') and (not inputs.submit_jobs_together):
        # Create the SLURM queue files
        create_SLURM_queue_files(times, inputs=inputs, debug=DEBUG)
        # Create the SLURM run script
        create_SLURM_run_script(time)
        # Send the script to the queue if requested
        run_job_script(inputs.run_script, filename="run_geos_SLURM.sh")
    elif (inputs.scheduler == 'SLURM') and (inputs.submit_jobs_together):
        # Create the SLURM queue files
        create_SLURM_queue_files(times, inputs=inputs, debug=DEBUG)
        # Create the SLURM run script
        create_SLURM_run_script2submit_together(times)
        # Send the script to the queue if requested
        filename = "run_geos_SLURM_queue_all_jobs.sh"
        run_job_script(inputs.run_script, filename=filename)


if __name__ == '__main__':
    main(debug=DEBUG)
