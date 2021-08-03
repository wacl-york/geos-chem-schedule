"""
Core functions for geos-chem-schedule
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
from utils import *


class GC_Job:
    """
    A class containing all the variables needed for scheduling a GC job

    Attributes
    -------
        attribute: default - description
        job_name: GEOS - Name of the job to appear in qstat
        step: month - Time of the split chunks
        queue_priority: 0 - Priority of the job (-1024 to 1023)
        queue_name: nodes - Name of the queue to submit too
        run_script_string: "yes" - Do you want to run the script immediately?
        out_of_hours_string: "yes" - Do you only want to run evenings and weekends?
        wall_time: "48:00:00" - How long will a chunk take (overestimate)
        send_email: yes - Do you want an email upon completion?
        email_address: "example@example.com"    - Address to send emails to
        email_setting: "e"   - Email on exit? google PBS email for more
        memory_need: "2Gb"   - Maximum memory you will need
        submit_jobs_together: True  - Submit jobs together+dependant on each other
        MetYear: "2016" - Year to use for meteorology (in HEMCO_Config.rc)?
        EmisYear: "2016" - Year to use for emissions (in HEMCO_Config.rc)?
        cpus_need: "20" - Number of CPUS to request per node?
        scheduler: "SLURM" - Scheduler (e.g. PBS, SLURM) to make scripts for?
        manage_hemco_files: "no" - mange the HEMCO_Config.rc file(s)?
    """

    def __init__(self):

        #        script_location = os.path.realpath(__file__)
        #        script_dir = os.path.dirname(script_location)
        dir = os.path.dirname(__file__)
        user_settings_file = os.path.join(dir, 'settings.json')

        # Set defaults
        self.cpus_need = '20'
        self.email_address = "example@example.com"
        self.email_setting = "e"
        self.EmisYear = "2016"
        self.job_name = "GEOS"
        self.queue_priority = "0"
        self.queue_name = "nodes"
        self.run_script_string = "yes"
        self.run_script = False
        self.scheduler = "SLURM"
        self.send_email = True
        self.submit_jobs_together = True
        self.step = "month"
        self.manage_hemco_files = False
        self.memory_need = "2Gb"
        self.MetYear = "2016"
        self.out_of_hours = False
        self.out_of_hours_string = "no"
        self.wall_time = "48:00:00"
        # Read the settings JSON file if this is present
        if os.path.exists(user_settings_file):
            settings_file = open(user_settings_file, 'r')
            options = json.load(settings_file)
        else:
            default = self.variables()
            settings_file = open(user_settings_file, 'w')
            json.dump(default, settings_file, sort_keys=True, indent=4)
            settings_file.close()
            options = default

        self.__dict__.update(options)
        return


def get_arguments(inputs, debug=False):
    """
    Get the arguments supplied from command line

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary
    debug (bool): Print debugging output to the screen

    Returns
    -------
    (GC_Job class)
    """
    # If there are no arguments then run the GUI
    if len(sys.argv) > 1:
        for arg in sys.argv:
            if "geos-chem-schedule" in arg:
                continue
            if arg.startswith("--setup"):
                setup_script()
            elif arg.startswith("--job-name="):
                inputs.job_name = (arg[11:].strip())[:9]
            elif arg.startswith("--step="):
                inputs.step = arg[7:].strip()
            elif arg.startswith("--queue-name="):
                inputs.queue_name = arg[13:].strip()
            elif arg.startswith("--queue-priority="):
                inputs.queue_priority = arg[17:].strip()
            elif arg.startswith("--submit="):
                inputs.run_script_string = arg[9:].strip()
            elif arg.startswith("--out-of-hours="):
                inputs.out_of_hours_string = arg[15:].strip()
            elif arg.startswith("--wall-time="):
                inputs.wall_time = arg[12:].strip()
            elif arg.startswith("--cpus-need="):
                inputs.cpus_need = arg[12:].strip()
            elif arg.startswith("--manage-hemco-files="):
                inputs.cpus_need = arg[21:].strip()
            elif arg.startswith("--submit-jobs-together="):
                inputs.cpus_need = arg[23:].strip()
            elif arg.startswith("--memory-need="):
                inputs.memory_need = arg[14:].strip()
            elif arg.startswith("--help"):
                print("""
            geos-chem-schedule.py

            For UI run without arguments
            Arguments are:
            --job-name=
            --step=
            --queue-name=
            --queue-priority=
            --submit=
            --out-of-hours=
            --wall-time=
            --submit-jobs-together=
            --manage-hemco-files=
            --memory-need=
            --cpus-need=
            e.g. to set the queue name to 'bob' write --queue-name=bob
            """)
        else:
            print("""Invalid argument {arg}
                     Try --help for more info.""".format(arg=arg)
                  )
            sys.exit(2)
    else:
        inputs = get_variables_from_cli(inputs)
    return inputs


def get_variables_from_cli(inputs):
    """
    Get the variables needed from a UI

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (GC_Job class)
    """
    # Set variables to values in the inputs class
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    EmisYear = inputs.EmisYear
    send_email = inputs.send_email
    wall_time = inputs.wall_time
    memory_need = inputs.memory_need
    MetYear = inputs.MetYear
    submit_jobs_together = inputs.submit_jobs_together
    manage_hemco_files = inputs.manage_hemco_files
    cpus_need = inputs.cpus_need
    step = inputs.step
    scheduler = inputs.scheduler

    DefaultInputPrtStr = "DEFAULT = {} :\n"

    # Say that No
    print('Using user interface to get geos-chem-schedule settings\n'
          'You can also configure the defaults via the settings.json file\n'
          'Or via '
          )

    # Name the queue
    clear_screen()
    print('What name do you want in the queue?\n',
          '(Will truncate to 9 characters).\n')
    input_read = str(input(DefaultInputPrtStr.format(job_name)))
    if input_read:
        job_name = input_read
    del input_read

    # Specify the step size
    clear_screen()
    PrtStr = "What time step size do you want? \n(6month recommended for 4x5, 2x25. 6month, 3month, 2month, week or day available).\n"
    print(PrtStr)
    input_read = str(input(DefaultInputPrtStr.format(step)))
    if input_read:
        step = input_read
    del input_read

    # Specify the scheduler
    # NOTE: this is get set in the settings.json file currently...
#     clear_screen()
#     PrtStr = "What scheduler is should the submission scripts be made for?\n"
#     print(PrtStr)
#     input_read = str(input(DefaultInputPrtStr.format(scheduler)))
#     if input_read:
#         scheduler = input_read
#     del input_read

    # Give the job a priority
    if scheduler == 'PBS':
        clear_screen()
        print("What queue priority do you want? (Between -1024 and 1023).\n")
        input_read = str(input(DefaultInputPrtStr.format(queue_priority)))
        if input_read:
            queue_priority = input_read
        del input_read

    # Choose the queue
    clear_screen()
    print("What queue do you want to go in?\n")
    input_read = str(input(DefaultInputPrtStr.format(queue_name)))
    if input_read:
        queue_name = input_read
    del input_read

    # Check for out of hours run
    if scheduler == 'PBS':
        clear_screen()
        print("Do you only want to run jobs out of normal work hours?\n"
              "(Monday to Friday 9am - 5pm)?\n")
        input_read = str(input(DefaultInputPrtStr.format(out_of_hours_string)))
        if input_read:
            out_of_hours_string = input_read
        del input_read

    # Set the walltime for the run
    clear_screen()
    print("How long does it take to run a month (HH:MM:SS)?\n",
          "Be generous! if the time is too short your\n"
          "job will get deleted (Max = 48 hours)\n")
    input_read = str(input(DefaultInputPrtStr.format(wall_time)))
    if input_read:
        wall_time = input_read
    del input_read

    # Submit all the jobs into the queue at the same time?
    clear_screen()
    print("Submit jobs all jobs together (subsequently dependant)?\n")
    input_read = str(input(DefaultInputPrtStr.format(submit_jobs_together)))
    if input_read:
        submit_jobs_together = input_read
    del input_read

    # Manage emissions and meteorological settings (HEMCO_Config.rc)
    clear_screen()
    print("Manage emissions & meteorological settings via HEMCO_Config.rc?\n")
    input_read = str(input(DefaultInputPrtStr.format(manage_hemco_files)))
    if input_read:
        manage_hemco_files = input_read
    del input_read
    # ... if so, then get the specific settings...
    if manage_hemco_files:
        print("What Met. year? (single number, list, or (+/-)offset)\n")
        input_read = str(input(DefaultInputPrtStr.format(MetYear)))
        if input_read:
            MetYear = input_read
        del input_read

        print("What emissions year? (single number, list, or (+/-)offset)\n")
        input_read = str(input(DefaultInputPrtStr.format(EmisYear)))
        if input_read:
            EmisYear = input_read
        del input_read

    # Set the memory requirements for the run
    clear_screen()
    print("How much memory does your run need?\n"
          "Lower amounts may increase priority.\n"
          "Example 2Gb, 4.8Gb, 200Mb, 200000kb.\n")
    input_read = str(input(DefaultInputPrtStr.format(memory_need)))
    if input_read:
        memory_need = input_read
    del input_read

    # Set the CPU requirements for the run
    clear_screen()
    print("How many CPUS (per node) does your run need?\n"
          "Lower amounts may increase priority.\n"
          "Example 2, 5, 15, 20.\n")
    input_read = str(input(DefaultInputPrtStr.format(cpus_need)))
    if input_read:
        cpus_need = input_read
    del input_read

    # Set the CPU requirements for the run
    clear_screen()
    print("Do you to be emailed on (final) job completion?\n"
          "if so, please type your email below.\n")
    PrtStr = "DEFAULT = {} - {}:\n"
    input_read = str(input(PrtStr.format(send_email, email_address)))
    if input_read:
        email_address = input_read
        send_email = True
    del input_read

    # Run script check
    clear_screen()
    print("Do you want to run the script now?\n")
    input_read = str(input(DefaultInputPrtStr.format(run_script_string)))
    if input_read:
        run_script_string = input_read
    del input_read

    clear_screen()

    # Update input variables
    inputs.cpus_need = cpus_need
    inputs.email_address = email_address
    inputs.EmisYear = EmisYear
    inputs.job_name = job_name
    inputs.queue_name = queue_name
    inputs.queue_priority = queue_priority
    inputs.run_script_string = run_script_string
    inputs.scheduler = scheduler
    inputs.send_email = send_email
    inputs.submit_jobs_together = submit_jobs_together
    inputs.manage_hemco_files = manage_hemco_files
    inputs.memory_need = memory_need
    inputs.MetYear = MetYear
    inputs.out_of_hours_string = out_of_hours_string
    inputs.step = step.lower()
    inputs.wall_time = wall_time
    return inputs


def list_of_times_to_run(start_time, end_time, inputs):
    """
    Create a list of start times and the end time of the run

    Parameters
    -------
    start_time (str): Start time of GEOS-Chem simulation in input.goes
    end_time (str):  End time of GEOS-Chem simulation in input.goes
    inputs (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (GC_Job class)
    """
    # Get steps from inputs
    step = inputs.step

    def datetime_2_YYYYMMDD(_my_datetime):
        """
        Get the geoschem YYYYMMDD from a datetime object.
        """
        return _my_datetime.strftime("%Y%m%d")

    if step == "12month":
        time_delta = relativedelta(months=12)
    elif step == "9month":
        time_delta = relativedelta(months=9)
    elif step == "6month":
        time_delta = relativedelta(months=6)
    elif step == "3month":
        time_delta = relativedelta(months=3)
    elif step == "2month":
        time_delta = relativedelta(months=2)
    elif step == "1month":
        time_delta = relativedelta(months=1)
    elif step == "month":
        time_delta = relativedelta(months=1)
    elif step == "2week":
        time_delta = relativedelta(weeks=2)
    elif step == "fortnight":
        time_delta = relativedelta(weeks=2)
    elif step == "1week":
        time_delta = relativedelta(weeks=2)
    elif step == "week":
        time_delta = relativedelta(weeks=1)
    elif step == "3day":
        time_delta = relativedelta(days=1)
    elif step == "1day":
        time_delta = relativedelta(days=1)
    elif step == "day":
        time_delta = relativedelta(days=1)

    start_datetime = datetime.datetime.strptime(start_time, "%Y%m%d")
    end_datetime = datetime.datetime.strptime(end_time, "%Y%m%d")

    _timestamp = (start_datetime)

    times = [datetime_2_YYYYMMDD(_timestamp)]
    while _timestamp < end_datetime:
        _timestamp = _timestamp + time_delta
        times.append(datetime_2_YYYYMMDD(_timestamp))

    return times


def update_output_line(line, end_time, inputs=None):
    """
    Make sure we have a 3 in the end date in input.geos output menu

    Parameters
    -------
    line (str): string pulled from the input file
    end_time (str):  End time of GEOS-Chem simulation (format: "YYYYMMDD")
    inputs (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (str)

    Notes
    -------
     - Returned output is the string to write to the *input.geos* file
    """
    # Retrieve the frequency of output
    step = inputs.step
    # if the output is in
    output_on_1st_of_month = False
    if 'month' in step.lower():
        output_on_1st_of_month = True

    # Get the name of the month
    _current_month_name = calendar.month_name[int(end_time[4:6])]
    _current_month_name = _current_month_name[0:3].upper()

    # Replace all instances of 3 with 0 so we only have the final day as 3
    line = line.replace('3', '0')

    # Get the position of the last day of simulations
    _current_day_of_month = int(end_time[6:8])
    _position = 26+_current_day_of_month

    if (line[20:23] == _current_month_name) or output_on_1st_of_month:
        newline = line[:_position-1] + '3' + line[_position:]
    else:
        newline = line

    return newline


def create_the_input_files(times, inputs=None, debug=False):
    """
    Create the input files for the run

    Parameters
    -------
    times (list): list of string times in the format YYYYMMDD
    inputs (GC_Job class): Class containing various inputs like a dictionary
    debug (bool): Print debugging output to the screen

    Returns
    -------
    (None)
    """
    # Create folder input files
    _dir = os.path.dirname("input_files/")
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    # Modify the input files to have the correct start times
    # Also make sure they end on a 3

    # Read the input file
    with open("input.geos", "r") as input_file:
        input_geos = input_file.readlines()

    for n_time, time in enumerate(times):
        end_time = time
        if time == times[0]:
            start_time = time
            continue

        filename = ".input.geos"
        time_input_file_location = os.path.join(_dir,
                                                (start_time+filename)
                                                )

        new_input_geos = create_new_input_file(start_time, end_time,
                                               input_geos, inputs=inputs)

        with open(time_input_file_location, 'w') as output_file:
            output_file.writelines(new_input_geos)

        # Also create files for controlling emissions via HEMCO
        if inputs.manage_hemco_files:
            filename = ".HEMCO_Config.rc"
            HEMCO_input_file_location = os.path.join(_dir,
                                                     (start_time+filename)
                                                     )
            # Get the original HEMCO_Config.rc file
            with open("HEMCO_Config.rc", "r") as input_HEMCO_file:
                input_HEMCO_geos = input_HEMCO_file.readlines()
            # Work on the emission and Met. year from input variables
            MetYear = inputs.MetYear
            EmisYear = inputs.EmisYear
            MetYear = get_HEMCO_year_from_var(MetYear, n_time, start_time)
            EmisYear = get_HEMCO_year_from_var(EmisYear, n_time, start_time)
            # Update MetYear, EmisYear, ...  variables
            new_HEMCO_input = create_new_HEMCO_input_file(input_HEMCO_geos,
                                                          MetYear=MetYear,
                                                          EmisYear=EmisYear)

            with open(HEMCO_input_file_location, 'w') as output_file:
                output_file.writelines(new_HEMCO_input)

        start_time = time
    return


def get_HEMCO_year_from_var(HEMCO_Year_var, n_time, start_time):
    """
    Convert a HEMCO year variable into a specific year

    Returns
    -------
    (int)

    Notes
    -------
     - A HEMCO year (e.g. EmisYear) can be provided as list, int, or offset
    """
    # Work out what the new MetYear should be...
    # If input variable is a single integer
    try:
        # Check the first value of the string is a value year number
        int(HEMCO_Year_var[0])
        # If so (e.g. not "+" or "-" return)
        return int(HEMCO_Year_var)
    except ValueError:
        pass
    # Or an offset from the modell year in input.geos
    try:
        start_year = int(start_time[:4])
        if '+' in HEMCO_Year_var:
            return start_year + int(HEMCO_Year_var.split('+')[-1])
        else:
            return start_year - int(HEMCO_Year_var.split('-')[-1])
    except ValueError:
        pass


def check_inputs(inputs, debug=False):
    """
    Make sure all the inputs

    Parameters
    -------
    Input (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (GC_Job class)
    """
    # Set variables from inputs
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    send_email = inputs.send_email
    wall_time = inputs.wall_time
    step = inputs.step
    # Earth0 queue names
#    queue_names = ['run', 'large',]
    # Viking queue names
    queue_names = [
        'interactive', 'month', 'week', 'gpu', 'himem_week', 'himem', 'test',
        'nodes',
    ]
    yes_list = ['yes', 'YES', 'Yes', 'Y', 'y', True, 'true', 'True']
    no_list = ['no', 'NO', 'No', 'N', 'n', False, 'false', 'False']
    steps = [
        "12month", "9month", "6month", "3month", "2month", "1month",
        "month", "fortnight", "2week", "1week", "week",
        "2day", "1day", "day",
    ]
    # Check steps string
    AssStr = "Unrecognised step size {step}.\ntry one of {steps}"
    assert (step in steps), AssStr.format(step=step, steps=steps)
    # Check Priority string
    AssStr = "Priority not between -1024 and 1023. Received {priority}"
    AssBool = (-1024 <= int(queue_priority) <= 1023)
    assert AssBool, AssStr.format(priority=queue_priority)
    # Check Queue type string
    AssStr = "Unrecognised queue type: {queue_name}\n try one of {}"
    assert (queue_name in queue_names), AssStr.format(queue_name=queue_name,
                                                      queue_names=queue_names)
    # Check out-of-hours queue option string
    AssStr = "Unrecognised option for out of hours.\nTry one of: {yes_list} / {no_list}\nThe command given was {run_script_string}"
    AssBool = ((out_of_hours_string in yes_list)
               or (out_of_hours_string in no_list))
    assert AssBool, AssStr.format(yes_list=yes_list, no_list=no_list,
                                  run_script_string=run_script_string)
    # Check 'run the script on completion' string
    AssStr = "Unrecognised option for run the script on completion.\nTry one of: {yes_list} / {no_list}\nThe command given was: {run_script_string}."
    AssBool = (run_script_string in yes_list) or (run_script_string in no_list)
    assert AssBool, AssStr.format(yes_list=yes_list, no_list=no_list,
                                  run_script_string=run_script_string)
    # Check email string
    AssStr = "Email option is neither yes or no. \nPlease check the settings. \nTry one of: {yes_list} / {no_list}"
    AssBool = (send_email in yes_list) or (send_email in no_list)
    assert AssBool, AssStr.format(yes_list=yes_list, no_list=no_list)

    # Create the logicals - run the script?
    if run_script_string in yes_list:
        #        inputs["run_script"] = True
        inputs.run_script = True
    elif run_script_string in no_list:
        inputs.run_script = False
    # Create the logicals - run only out of hours?
    if out_of_hours_string in yes_list:
        inputs.out_of_hours = True
    elif out_of_hours_string in no_list:
        inputs.out_of_hours = False
    # Create the logicals - Send an email?
    if send_email in yes_list:
        inputs.send_email = True
    elif send_email in no_list:
        inputs.send_email = False
    return inputs


def create_new_input_file(start_time, end_time, input_file, inputs=None):
    """
    Create a new input file based on the passed in open input file.

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary
    start_time (str): Start of GEOS-Chem run in format YYYYMMSS
    end_time (str): End of GEOS-Chem run in format YYYYMMSS
    inputs (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (list)

    Notes
    -------
     - Return list is the output file as a list of strings
    """
    new_lines = []
    # Change the lines that need changing by reading their start date
    for line in input_file:
        if line.startswith("Start YYYYMMDD"):
            newline = line[:26] + str(start_time) + line[34:]
            # Confirm the run starts on the first of the time
        elif line.startswith("End   YYYYMMDD"):
            newline = line[:26] + str(end_time) + line[34:]
        # Force CSPEC on
        elif line.startswith("Read and save CSPEC_FULL:"):
            newline = line[:26] + 'T\n'
        # Make sure write at end on a 3
        elif line.startswith("Schedule output for"):
            newline = update_output_line(line, end_time, inputs=inputs)
        else:
            newline = line
        new_lines.append(newline)
    return new_lines


def create_new_HEMCO_input_file(input_file, EmisYear="2016", MetYear="2016",
                                debug=False):
    """
    Create a new HEMCO_Config.rc input file based on the passed file.

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary
    start_time (str): Start of GEOS-Chem run in format YYYYMMSS
    end_time (str): End of GEOS-Chem run in format YYYYMMSS
    inputs (GC_Job class): Class containing various inputs like a dictionary

    Returns
    -------
    (list)

    Notes
    -------
     - Return list is the output file as a list of strings
    """

    new_lines = []
    if debug:
        print('MetYear: {}, EmisYear: {}'.format(MetYear, EmisYear))

    # Change the lines that need changing by reading their start date
    for line in input_file:
        # Update year for emissions
        if line.startswith("Emission year: "):
            buffer = ' '*(len(line.split(':')[-1])-5)
            newline = line[:14] + buffer + str(EmisYear) + '\n'
        elif line.startswith("EmisYear: "):
            buffer = ' '*(len(line.split(':')[-1])-5)
            newline = line[:9] + buffer + str(EmisYear) + '\n'
        # Update MetYear
        elif line.startswith("MetYear: "):
            buffer = ' '*(len(line.split(':')[-1])-5)
            newline = line[:8] + buffer + str(MetYear) + '\n'
        else:
            newline = line
        new_lines.append(newline)
    return new_lines


def create_PBS_queue_files(times, inputs=None, debug=False):
    """
    Create the queue files for a PBS managed queue (York's earth0 HPC)

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary
    times (list): list of string times in the format YYYYMMDD
    debug (bool): Print debugging output to the screen

    Returns
    -------
    (None)
    """
    # Create local variables
    queue_name = inputs.queue_name
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    out_of_hours = inputs.out_of_hours
    wall_time = inputs.wall_time
    send_email = inputs.send_email
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    memory_need = inputs.memory_need
    cpus_need = inputs.cpus_need

    # Create folder queue files
    _dir = os.path.dirname("PBS_queue_files/")
    if not os.path.exists(_dir):
        os.makedirs(_dir)

    # Modify the input files to have the correct start months
    for time in times:
        end_time = time
        if time == times[0]:
            start_time = time
            continue

        # Make the out of hours string if only running out of hours
        if out_of_hours:
            out_of_hours_string = (
                """
 if ! ( $out_of_hours_overide ); then
    if $out_of_hours ; then
       if [ $(date +%u) -lt 6 ]  && [ $(date +%H) -gt 8 ] && [ $(date +%H) -lt 17 ] ; then
          job_number=$(qsub -a 1810 PBS_queue_files/{start_time}.pbs)
          echo $job_number
          echo qdel $job_number > exit_geos.sh
          echo "Tried running in work hours but we don't want to. Will try again at 1800. The time we attempted to run was:">>logs/log.log
          echo $(date)>>logs/log.log
          exit 1
       fi
    fi
 fi
 """
            ).format(start_time=start_time)
        else:
            out_of_hours_string = "\n"

        # Set up email if its the final run and email = True
        # TODO - add an option to always send email when run finishes?
        # or if run finishes without a success code?
        if send_email and (time == times[-1]):
            email_string = (
                """
#PBS -m {email_setting}
#PBS -M {email_address}
"""
            ).format(email_setting=email_setting,
                     email_address=email_address)
        else:
            email_string = "\n"
        # Setup queue file string
        templates_dir = '{}/templates/'.format(os.path.dirname(__file__))
        script_template_file = 'PBS_queue_script_template'
        script_template = os.path.join(templates_dir, script_template_file)
        with open(script_template, 'r') as template_file:
            queue_file_string = [i for i in template_file]
        queue_file_string = (''.join(queue_file_string))
        # Add all the variables to the string
        queue_file_string = queue_file_string.format(
            queue_name=queue_name,
            # job name can only be 15 characters
            job_name=(job_name + start_time)[:14],
            start_time=start_time,
            wall_time=wall_time,
            memory_need=memory_need,
            cpus_need=cpus_need,
            queue_priority=queue_priority,
            email_string=email_string,
            out_of_hours_string=out_of_hours_string,
            end_time=end_time
        )
        # Write the queue file string
        queue_file_location = os.path.join(_dir, (start_time + ".pbs"))
        queue_file = open(queue_file_location, 'w')
        queue_file.write(queue_file_string)
        # If this is the final month then run an extra command
        if time == times[-1]:
            run_completion_script()
        queue_file.close()
        # Change the permissions so it is executable
        st = os.stat(queue_file_location)
        os.chmod(queue_file_location, st.st_mode | stat.S_IEXEC)
        # Now update the start_time variable
        start_time = time
    return


def create_SLURM_queue_files(times, inputs=None, debug=False):
    """
    Create the queue files for a SLURM managed queue (e.g. York's viking HPC)

    Parameters
    -------
    inputs (GC_Job class): Class containing various inputs like a dictionary
    times (list): list of string times in the format YYYYMMDD
    debug (bool): Print debugging output to the screen

    Returns
    -------
    (None)
    """
    # Create local variables
    cpus_need = inputs.cpus_need
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    job_name = inputs.job_name
    manage_hemco_files = inputs.manage_hemco_files
    memory_need = inputs.memory_need
    out_of_hours = inputs.out_of_hours
    queue_name = inputs.queue_name
    queue_priority = inputs.queue_priority
    send_email = inputs.send_email
    submit_jobs_together = inputs.submit_jobs_together
    wall_time = inputs.wall_time

    # Print received settings to debug:
    if debug:
        print('scheduler:', inputs.scheduler)
        print('cpus_need:', cpus_need)
        print('email_address:', email_address)
        print('email_setting:', email_setting)
        print('job_name:', job_name)
        print('manage_hemco_files:', manage_hemco_files)
        print('memory_need:', memory_need)
        print('out_of_hours:', out_of_hours)
        print('queue_name:', queue_name)
        print('queue_priority:', queue_priority)
        print('send_email:', send_email)
        print('submit_jobs_together:', submit_jobs_together)
        print('wall_time:', wall_time)

    # Create folder queue files
    _dir = os.path.dirname("SLURM_queue_files/")
    if not os.path.exists(_dir):
        os.makedirs(_dir)

    # Setup variables to hold various Text options
    # ... hardwired capitalised variables for
    slurm_capital_variables = """# CHANGE TO GEOS-Chem run directory, assuming job was submitted from there:
cd \"${SLURM_SUBMIT_DIR}\" || exit 1

# Set OpenMP thread count to number of cores requested for job:
export OMP_NUM_THREADS=\"${SLURM_CPUS_PER_TASK}\""""

    # Modify the input files to have the correct start months
    for time in times:
        end_time = time
        if time == times[0]:
            start_time = time
            continue

        # Make the out of hours string if only running out of hours
        # TODO - set this up with SLURM
        if out_of_hours:
            out_of_hours_string = "\n"
        else:
            out_of_hours_string = "\n"
        # Set up email if its the final run and email = True
        # TODO - add an option to always send email when run finishes?
        # or if run finishes without a success code?
        if send_email and (time == times[-1]):
            email_address2use = email_address
        else:
            email_address2use = "TEST@TEST.com"
        # Setup final lines for submission script - call the next on or stop?
        if time == times[-1]:
            submit_next_job = 'False'
        else:
            submit_next_job = 'True'
        # If submitting jobs to queue together (dependently), then override
        if submit_jobs_together:
            submit_next_job = 'False'

        # Setup queue file string
        templates_dir = '{}/templates/'.format(os.path.dirname(__file__))
        script_template_file = 'SLURM_queue_script_template'

        script_template = os.path.join(templates_dir, script_template_file)
        with open(script_template, 'r') as template_file:
            queue_file_string = [i for i in template_file]
        queue_file_string = (''.join(queue_file_string))
        # If debugging, print loop to screen by date
        if debug:
            print('times: {}'.format(times))
            print('time: {} of n={} '.format(time, len(times)))
            print('time == times[-1]: {}'.format((time == times[-1])))
            print('start_time, {} end_time: {}'.format(start_time, end_time))
            print('send_email: {}'.format(send_email))
            print('email_address: {}'.format(email_address2use))
            print('templates_dir: {}'.format(templates_dir))
        # Setup lines to manage HEMCO files(s) if this was requested
        if manage_hemco_files:
            HEMCO_file_lines = """
    rm -f HEMCO_Config.rc
    ln -s input_files/{start_time}.HEMCO_Config.rc HEMCO_Config.rc\n
     """
            HEMCO_file_lines = HEMCO_file_lines.format(start_time=start_time)
        else:
            HEMCO_file_lines = "\n"
        # Add all the variables to the string
        queue_file_string = queue_file_string.format(
            queue_name=queue_name,
            # job name can only be 15 characters
            job_name=(job_name + start_time)[:14],
            start_time=start_time,
            wall_time=wall_time,
            memory_need=memory_need,
            submit_jobs_together=submit_jobs_together,
            cpus_need=cpus_need,
            queue_priority=queue_priority,
            out_of_hours_string=out_of_hours_string,
            end_time=end_time,
            email_address=email_address2use,
            slurm_capital_variables=slurm_capital_variables,
            HEMCO_file_lines=HEMCO_file_lines,
            submit_next_job=submit_next_job,
        )
        # Write the queue file to disk
        queue_file_location = os.path.join(_dir, (start_time + ".sbatch"))
        if debug:
            print('queue_file_location: {}'.format(queue_file_location))
            print('queue_file_string: {}'.format(queue_file_string))
        queue_file = open(queue_file_location, 'w')
        queue_file.write(queue_file_string)
        # If this is the final month then run an extra command
        if time == times[-1]:
            run_completion_script()
        queue_file.close()
        # Change the permissions so it is executable
        st = os.stat(queue_file_location)
        os.chmod(queue_file_location, st.st_mode | stat.S_IEXEC)
        # Now update the start_time variable
        start_time = time
    return


def create_PBS_run_script(time):
    """
    Create the script that can set the 1st scheduled job running

    Parameters
    -------
    time (str): string time to run job script for in the format YYYYMMDD

    Returns
    -------
    (None)
    """
    FileName = 'run_geos_PBS.sh'
    run_script = open(FileName, 'w')
    run_script_string = ("""
#!/bin/bash
qsub PBS_queue_files/{time}.pbs
     """)
    run_script.write(run_script_string.format(time=time))
    run_script.close()
    # Change the permissions so it is executable
    st = os.stat(FileName)
    os.chmod(FileName, st.st_mode | stat.S_IEXEC)
    return


def create_SLURM_run_script(time):
    """
    Create the script that can set the 1st scheduled job running

    Parameters
    -------
    time (str): string time to run job script for in the format YYYYMMDD

    Returns
    -------
    (None)
    """
    FileName = 'run_geos_SLURM.sh'
    run_script = open(FileName, 'w')
    run_script_string = ("""
#!/bin/bash
job_number=$(sbatch SLURM_queue_files/{time}.sbatch)
echo "$job_number"
     """)
    run_script.write(run_script_string.format(time=time))
    run_script.close()
    # Change the permissions so it is executable
    st = os.stat(FileName)
    os.chmod(FileName, st.st_mode | stat.S_IEXEC)
    return


def create_SLURM_run_script2submit_together(times):
    """
    Create the script that can set the 1st scheduled job running

    Parameters
    -------
    time (str): string time to run job script for in the format YYYYMMDD

    Returns
    -------
    (None)
    """
    print(times)
    FileName = 'run_geos_SLURM_queue_all_jobs.sh'
    run_script = open(FileName, 'w')
    Line0 = "#!/bin/bash \n"
    Line1 = """job_num_{time}=$(sbatch --parsable SLURM_queue_files/{time}.sbatch) \n"""
    Line2 = """echo "$job_num_{time}" \n"""
    Line3 = """job_num_{time2}=$(sbatch --parsable --dependency=afterok:"$job_num_{time1}" SLURM_queue_files/{time2}.sbatch) \n"""
    for n_time, time in enumerate(times[:-1]):
        #
        if time == times[0]:
            run_script.write(Line0)
            run_script.write(Line1.format(time=time))
            run_script.write(Line2.format(time=time))
        else:
            run_script.write(Line3.format(time1=times[n_time-1], time2=time))
            run_script.write(Line2.format(time=time))
    run_script.close()
    # Change the permissions so it is executable
    st = os.stat(FileName)
    os.chmod(FileName, st.st_mode | stat.S_IEXEC)
    return


def run_job_script(run_script, filename="run_geos_SLURM.sh"):
    """
    Call the scheduler run script with a subprocess command
    """
    if run_script:
        subprocess.call(["bash", filename])
    return


def run_completion_script():
    """
    Run a script when the final job finishes

    Notes
    -------
     - For example, this could be a clean up script or a post-processing script
    """
    return
