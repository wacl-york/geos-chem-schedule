#!/usr/bin/env python
"""
A script to split up GEOS-Chem jobs on HPC into shorter runs


This allows fitting in smaller queues and fairer access.
The jobs can call the next job in the sequence meaning you can submit in the
same way.
 - file name = monthly_run.py
There are also options that you can either pass as arguments or run a UI
if no arguments are passed.
see "$ monthly_run.py --help" for more information.
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

# Master debug switch for main driver
DEBUG = True
#Scheduler = 'PBS'
Scheduler = 'SLURM'

######
# To do
# Write complete test suit
# Migrate from class to dictionary when tests are complete
########


class GET_INPUTS:
    """
    A class containing all the variables needed
    Attributes:
        attribute: default   - description
        job_name: GEOS   - Name of the job to appear in qstat
        step: month   - Time of the split chunks
        queue_priority: 0   - Priority of the job (-1024 to 1023)
        queue_name: nodes   - Name of the queue to submit too
        run_script_string: yes   - Do you want to run the script immediately
        out_of_hours_string: yes   - Do you only want to run evenings and weekends?
        wall_time: "48:00:00"   - How long will a chunk take (overestimate)
        email_option: "yes"   - Do you want an email upon completion?
        email_address: "example@example.com"    - Address to send emails to
        email_setting: "e"   - Email on exit? google PBS email for more
        memory_need: "20gb"   - Maximum memory you will need"
    """

    def __init__(self):

        script_location = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_location)
        user_settings_file = os.path.join(script_dir, 'settings.json')

        # set defaults
        self.job_name = "GEOS"
        self.step = "month"
        self.queue_priority = "0"
        self.queue_name = "nodes"
        self.run_script_string = "yes"
        self.out_of_hours_string = "no"
        self.wall_time = "48:00:00"
        self.email_option = "yes"
        self.email_address = "example@example.com"
        self.email_setting = "e"
        self.memory_need = "20gb"
        self.cpus_need = '20'
        self.run_script = False
        self.out_of_hours = False
        self.email = False

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

    def __str__(self):
        """
        Set the method to print the class
        """
        _vars = self.variables()
        string = ""
        for key, val in list(_vars.items()):
            string = string + "{key}: {val} \n".format(key=key, val=val)
            print("{key}: {val} \n".format(key=key, val=val))
#        attrs = str(self.__dict__)
        return string

    def __repr__(self):
        """
        Set the method to print the class
        """
        attrs = str(self.__dict__)
        return attrs

    def variables(self):
        """
        Return a list of class variables as a dictionary.
        """
        def built_in_function(key):
            """
            Check if a key is a built in class function or variable
            """
            return bool(key.startswith('__') and not callable(key))
        _vars = {key: val for key, val in list(
            self.__dict__.items()) if not built_in_function(key)}
        return _vars

    def __setitem__(self, name, value):
        self.__dict__[name] = value

    def __getitem__(self, name):
        """Allow use like a dictionary"""
        return self.__dict__[name]


def run_completion_script():
    """
    Run a script when the final month finishes. This could be a clean up script
    or a post processing script.
    """
    return

# --------------------------------------------------------------
# Nothing below here should need changing, but feel free to look.


def main(debug=DEBUG):
    """
    Run monthly_run
    """
    # Get the default inputs as a class
    inputs = GET_INPUTS()

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

    # Create the PBS queue files
    create_PBS_queue_files(times, inputs=inputs, debug=DEBUG)

    # Create the SLURM queue files
    create_SLURM_queue_files(times, inputs=inputs, debug=DEBUG)

    # Create the PSB run script
    create_PBS_run_script(times)

    # Create the SLURM run script
    create_SLURM_run_script(times)

    # Send the script to the queue if requested
    if Scheduler == 'PBS':
        run_PBS_script(inputs.run_script)

    # Send the script to the queue if requested
    if Scheduler == 'SLURM':
        run_SLURM_script(inputs.run_script)

    return


def check_inputs(inputs, debug=False):
    """
    Make sure all the inputs make sense

    Input: inputs(dictionary)
    Output: inputs(dictionary)
    """
    # Set variables from inputs
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    email_option = inputs.email_option
    wall_time = inputs.wall_time
    step = inputs.step
    # Earth0 queue names
#    queue_names = ['run', 'large',]
    # Viking queue names
    queue_names = [
        'interactive', 'month', 'week', 'gpu', 'himem_week', 'himem', 'test',
        'nodes',
    ]
    yess = ['yes', 'YES', 'Yes', 'Y', 'y']
    nooo = ['no', 'NO', 'No', 'N', 'n']
    steps = [
        "12month", "6month", "3month", "2month", "1month",  "month", "week",
        "day"
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
    AssStr = "Unrecognised option for out of hours.\nTry one of: {yess} / {nooo}\nThe command given was {run_script_string}"
    AssBool = ((out_of_hours_string in yess) or (out_of_hours_string in nooo))
    assert AssBool, AssStr.format(yess=yess, nooo=nooo,
                                  run_script_string=run_script_string)
    # Check 'run the script on completion' string
    AssStr = "Unrecognised option for run the script on completion.\nTry one of: {yess} / {nooo}\nThe command given was: {run_script_string}."
    AssBool = (run_script_string in yess) or (run_script_string in nooo)
    assert AssBool, AssStr.format(yess=yess, nooo=nooo,
                                  run_script_string=run_script_string)
    # Check email string
    AssStr = "Email option is neither yes or no. \nPlease check the settings. \nTry one of: {yess} / {nooo}"
    AssBool = (email_option in yess) or (email_option in nooo)
    assert AssBool, AssStr.format(yess=yess, nooo=nooo)

    # Create the logicals - run the script?
    if run_script_string in yess:
        inputs["run_script"] = True
    elif run_script_string in nooo:
        inputs["run_script"] = False
    # Create the logicals - run only out of hours?
    if out_of_hours_string in yess:
        inputs["out_of_hours"] = True
    elif out_of_hours_string in nooo:
        inputs["out_of_hours"] = False
    # Create the logicals - Send an email?
    if email_option in yess:
        inputs["email"] = True
    elif email_option in nooo:
        inputs["email"] = False
    return inputs


def backup_the_input_file():
    """
    Save a copy of the origional input file
    """
    input_file = "input.geos"
    backup_input_file = "input.geos.orig"
    if not os.path.isfile(backup_input_file):
        shutil.copyfile(input_file, backup_input_file)
    return


def setup_script():
    """
    Creates a symbolic link allowing the use to run "monthly_run" from any folder"
    """
    print("\n",
          "Monthly run setup complete. Change your default settings in settings.json\n",
          "To run the script from anywhere with the monthly_run command,",
          "copy the following code into your terminal. \n")

    script_location = os.path.realpath(__file__)
    # make sure the script is excecutable
    print("chmod 755 {script}".format(script=script_location))
    # Make sure there is a ~/bin file
    print("mkdir -p $HOME/bin")
    # Create a symlink from the file to the bin
    print("ln -s {script} $HOME/bin/monthly_run".format(script=script_location))
    # Make sure the ~/bin is in the bashrc
    # with open('$HOME/.bashrc','a') as bashrc:
    #        bashrc.write('## Written by monthly_run')
    #        bashrc.write('export PATH=$PATH:$HOME/bin')
    print('echo "## Written by monthly_run" >> $HOME/.bashrc')
    print('echo "export PATH=\$PATH:\$HOME/bin" >> $HOME/.bashrc')
    # source the bashrc
    print("source $HOME/.bashrc")
    print("\n")
    sys.exit()


def get_arguments(inputs, debug=DEBUG):
    """
    Get the arguments supplied from command line
    """
    # If there are no arguments then run the GUI
    if len(sys.argv) > 1:
        for arg in sys.argv:
            if "monthly-run" in arg:
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
            elif arg.startswith("--memory-need="):
                inputs.wall_time = arg[14:].strip()
            elif arg.startswith("--help"):
                print("""
            monthly-run.py

            For UI run without arguments
            Arguments are:
            --job-name=
            --step=
            --queue-name=
            --queue-priority=
            --submit=
            --out-of-hours=
            --wall-time=
            --memory-need=
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


def test_get_arguments():
    """
    Test that the passed arguments get assigned to the class.
    """
    ########
    # TO DO
    ########
    #
    # Write these tests....
    #
    ########
    return


def get_variables_from_cli(inputs):
    """
    Get the variables needed from a UI
    """
    # Set variables from inputs
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    wall_time = inputs.wall_time
    memory_need = inputs.memory_need
    cpus_need = inputs.cpus_need
    step = inputs.step

    # Name the queue
    clear_screen()
    print('What name do you want in the queue?\n',
          '(Will truncate to 9 characters).\n')
    input_read = str(input('DEFAULT = ' + job_name + ' :\n'))
    if input_read:
        job_name = input_read
    del input_read

    # Specify the step size
    clear_screen()
    PrtStr = "What time step size do you want? \n(month recommended for 4x5, 2x25. 6month, 3month, 2month, week or day available).\n"
    print(PrtStr)
    input_read = str(input('DEFAULT = ' + step + ' :\n'))
    if input_read:
        step = input_read
    del input_read

    # Give the job a priority
    clear_screen()
    print("What queue priority do you want? (Between -1024 and 1023).\n")
    input_read = str(input('DEFAULT = ' + queue_priority + ' :\n'))
    if input_read:
        queue_priority = input_read
    del input_read

    # Choose the queue
    clear_screen()
    print("What queue do you want to go in?\n")
    input_read = str(input('DEFAULT = ' + queue_name + ' :\n'))
    if input_read:
        queue_name = input_read
    del input_read

    # Check for out of hours run
    clear_screen()
    print("Do you only want to run jobs out of normal work hours?\n"
          "(Monday to Friday 9am - 5pm)?\n")
    input_read = str(input('Default = ' + out_of_hours_string + ' :\n'))
    if input_read:
        out_of_hours_string = input_read
    del input_read

    # Set the walltime for the run
    clear_screen()
    print("How long does it take to run a month (HH:MM:SS)?\n",
          "Be generous! if the time is too short your\n"
          "job will get deleted (Max = 48 hours)\n")
    input_read = str(input('DEFAULT = ' + wall_time + ' :\n'))
    if input_read:
        wall_time = input_read
    del input_read

    # Set the memory requirements for the run
    clear_screen()
    print("How much memory does your run need?\n"
          "Lower amounts may increase priority.\n"
          "Example 4gb, 200mb, 200000kb.\n")
    input_read = str(input('DEFAULT = ' + memory_need + ' :\n'))
    if input_read:
        memory_need = input_read
    del input_read

    # Set the CPU requirements for the run
    clear_screen()
    print("How many CPUS does your run need?\n"
          "Lower amounts may increase priority.\n"
          "Example 5, 15, 20.\n")
    input_read = str(input('DEFAULT = ' + cpus_need + ' :\n'))
    if input_read:
        cpus_need = input_read
    del input_read

    # Run script check
    clear_screen()
    print("Do you want to run the script now?\n")
    input_read = str(input('DEFAULT = ' + run_script_string + ' :\n'))
    if input_read:
        run_script_string = input_read
    del input_read

    clear_screen()

    # Update input variables
    inputs.job_name = job_name
    inputs.queue_name = queue_name
    inputs.queue_priority = queue_priority
    inputs.run_script_string = run_script_string
    inputs.out_of_hours_string = out_of_hours_string
    inputs.wall_time = wall_time
    inputs.memory_need = memory_need
    inputs.cpus_need = cpus_need
    inputs.step = step.lower()
    return inputs


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


def run_PBS_script(run_script):
    """
    Call the PBS run script with a subprocess command
    """
    if run_script:
        subprocess.call(["bash", "run_geos_PBS.sh"])
    return


def run_SLURM_script(run_script):
    """
    Call the SLURM run script with a subprocess command
    """
    if run_script:
        subprocess.call(["bash", "run_geos_SLURM.sh"])
    return


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


def list_of_times_to_run(start_time, end_time, inputs):
    """
    Create a list of start times and the end time of the run
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
    if step == "6month":
        time_delta = relativedelta(months=6)
    if step == "3month":
        time_delta = relativedelta(months=3)
    if step == "2month":
        time_delta = relativedelta(months=2)
    if step == "1month":
        time_delta = relativedelta(months=1)
    if step == "month":
        time_delta = relativedelta(months=1)
    elif step == "week":
        time_delta = relativedelta(weeks=1)
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

    INPUT:
        line - string pulled from the input file
        end_time - "YYYYMMDD"
    OUTPUT:
        line - string to write to the input file
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


def is_current_year_a_leap_year():
    """ Check if current year is a leap year """
    # TODO
    return


def create_the_input_files(times, inputs=None, debug=False):
    """
    Create the input files for the run
    INPUT:
        times - list of string times in the format YYYYMMDD
    Output:
        None
    """
    # create folder input files
    _dir = os.path.dirname("input_files/")
    if not os.path.exists(_dir):
        os.makedirs(_dir)

    # Modify the input files to have the correct start times
    # Also make sure they end on a 3

    # Read the input file
    with open("input.geos", "r") as input_file:
        input_geos = input_file.readlines()

    for time in times:
        end_time = time
        if time == times[0]:
            start_time = time
            continue

        time_input_file_location = os.path.join( _dir,
                                                (start_time+".input.geos")
                                                )

        new_input_geos = create_new_input_file(start_time, end_time,
                                               input_geos, inputs=inputs)

        with open(time_input_file_location, 'w') as output_file:
            output_file.writelines(new_input_geos)

#
#
#        for line in input_geos:
#
#            if line.startswith("Start YYYYMMDD"):
#                newline = line[:26] + str(start_time) + line[34:]
#                output_file.write(newline)
#                # Confirm the run starts on the first of the time
#            elif line.startswith("End   YYYYMMDD"):
#                newline = line[:26] + str(end_time) + line[34:]
#                output_file.write(newline)
#            # Force CSPEC on
#            elif line.startswith("Read and save CSPEC_FULL:"):
#                newline = line[:26] + 'T \n'
#                output_file.write(newline)
#            # Make sure write at end on a 3
#            elif line.startswith("Schedule output for"):
#                newline = update_output_line( line, end_time )
#                output_file.write(newline)
#            else:
#                newline = line
#                output_file.write(newline)
#        output_file.close()
#        input_geos.close()
        start_time = time
    return


def create_new_input_file(start_time, end_time, input_file, inputs=None):
    """
    Create a new input file based on the passed in open input file.

    INPUTS:
        start_time: YYYYMMSS
        end_time: YYYYMMSS
        input_file: Open file that is a list of strings
    OUTPUT:
        output_file: output file that is a list of strings
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


def create_PBS_queue_files(times, inputs=None, debug=DEBUG):
    """
    Create the queue files for a PBS managed queue (York's earth0 HPC)
    """
    # Create local variables
    queue_name = inputs.queue_name
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    out_of_hours = inputs.out_of_hours
    wall_time = inputs.wall_time
    email = inputs.email
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
        if email and (time == times[-1]):
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
        script_location = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_location)
        script_template_file = 'PBS_queue_script_template'
        script_template = os.path.join(script_dir, script_template_file)
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

        queue_file_location = os.path.join(_dir, (start_time + ".pbs"))
        queue_file = open(queue_file_location, 'w')
        queue_file.write(queue_file_string)
        # If this is the final month then run an extra command
        if time == times[-1]:
            run_completion_script()
        queue_file.close()
        # Change the permissions so it is executable
        st = os.stat( queue_file_location )
        os.chmod( queue_file_location, st.st_mode | stat.S_IEXEC)
        # Now update the start_time variable
        start_time = time
    return


def create_SLURM_queue_files(times, inputs=None, debug=DEBUG):
    """
    Create the queue files for a SLURM managed queue (e.g. York's viking HPC)
    """
    # Create local variables
    queue_name = inputs.queue_name
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    out_of_hours = inputs.out_of_hours
    wall_time = inputs.wall_time
    email = inputs.email
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    memory_need = inputs.memory_need
    cpus_need = inputs.cpus_need

    # Create folder queue files
    _dir = os.path.dirname("SLURM_queue_files/")
    if not os.path.exists(_dir):
        os.makedirs(_dir)

    # Modify the input files to have the correct start months
    for time in times:
        end_time = time
        if time == times[0]:
            start_time = time
            continue

        # Make the out of hours string if only running out of hours
        # TODO - set this up with SLURM
        if out_of_hours:
#             out_of_hours_string = (
#                 """
#  if ! ( $out_of_hours_overide ); then
#     if $out_of_hours ; then
#        if [ $(date +%u) -lt 6 ]  && [ $(date +%H) -gt 8 ] && [ $(date +%H) -lt 17 ] ; then
#           job_number=$(qsub -a 1810 PBS_queue_files/{start_time}.pbs)
#           echo $job_number
#           echo qdel $job_number > exit_geos.sh
#           echo "Tried running in work hours but we don't want to. Will try again at 1800. The time we attempted to run was:">>logs/log.log
#           echo $(date)>>logs/log.log
#           exit 1
#        fi
#     fi
#  fi
#  """
#             ).format(start_time=start_time)
            out_of_hours_string = "\n"
        else:
            out_of_hours_string = "\n"

        # Set up email if its the final run and email = True
        # TODO - add an option to always send email when run finishes?
        # or if run finishes without a success code?
        if email and (time == times[-1]):
#             email_string = (
#                 """
# #PBS -m {email_setting}
# #PBS -M {email_address}
# """
#             ).format(email_setting=email_setting,
#                      email_address=email_address)
            email_address = email_address
        else:
            email_address = "TEST@TEST.com"
        # Also copy in the hardwired capitalised variables for now
        slurm_capital_variables = """# CHANGE TO GEOS-Chem run directory, assuming job was submitted from there:
cd \"${SLURM_SUBMIT_DIR}\" || exit 1

# Set OpenMP thread count to number of cores requested for job:
export OMP_NUM_THREADS=\"${SLURM_CPUS_PER_TASK}\""""
        # Setup queue file string
        script_location = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_location)
        script_template_file = 'SLURM_queue_script_template'
        script_template = os.path.join(script_dir, script_template_file)
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
            out_of_hours_string=out_of_hours_string,
            end_time=end_time,
            email_address=email_address,
            slurm_capital_variables=slurm_capital_variables,
        )

        queue_file_location = os.path.join(_dir, (start_time + ".sbatch"))
        queue_file = open(queue_file_location, 'w')
        queue_file.write(queue_file_string)
        # If this is the final month then run an extra command
        if time == times[-1]:
            run_completion_script()
        queue_file.close()
        # Change the permissions so it is executable
        st = os.stat( queue_file_location )
        os.chmod( queue_file_location, st.st_mode | stat.S_IEXEC)
        # Now update the start_time variable
        start_time = time
    return


def create_PBS_run_script(months):
    """
    Create the script that can set the run jobs running

    Input: months
    Output: 'run_PBS_geos.sh'
    """
    FileName = 'run_geos_PBS.sh'
    run_script = open(FileName, 'w')
    run_script_string = ("""
#!/bin/bash
qsub PBS_queue_files/{month}.pbs
     """).format(month=months[0])
    run_script.write(run_script_string)
    run_script.close()
    # Change the permissions so it is executable
    st = os.stat( FileName )
    os.chmod( FileName, st.st_mode | stat.S_IEXEC)
    return


def create_SLURM_run_script(months):
    """
    Create the script that can set the run jobs running
    Input: months
    Output: 'run_SLURM_geos.sh'
    """
    FileName = 'run_geos_SLURM.sh'
    run_script = open(FileName, 'w')
    run_script_string = ("""
#!/bin/bash
sbatch SLURM_queue_files/{month}.sbatch
     """).format(month=months[0])
    run_script.write(run_script_string)
    run_script.close()
    # Change the permissions so it is executable
    st = os.stat( FileName )
    os.chmod( FileName, st.st_mode | stat.S_IEXEC)
    return


# --------------------------------------------------------------


if __name__ == '__main__':
    main(debug=DEBUG)


########
# Tests
########

def test_check_inputs():
    """
    Test check_inputs()
    """

    yess = ['yes', 'YES', 'Yes', 'Y', 'y']
    nooo = ['NO', 'no', 'NO', 'No', 'N', 'n']

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
        "valid_data": yess + nooo,
        "invalid_data": ["bob"],
        "data_logical": "out_of_hours"
    }
    email_option = {
        "name": "email_option",
        "valid_data": yess + nooo,
        "invalid_data": ["bob", 1000],
        "data_logical": "email"
    }
    run_script_string = {
        "name": "run_script_string",
        "valid_data": yess + nooo,
        "invalid_data": ["bob"],
        "data_logical": "run_script"
    }
    steps = {
        "name": "step",
        "valid_data": ["month", "week", "day"],
        "invalid_data": ["bob"],
    }

    tests = [queue_priority, queue_name, out_of_hours_string,
             email_option, run_script_string, steps]

    for test in tests:
        for data in test["valid_data"]:
            inputs = GET_INPUTS()
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
                if data in yess:
                    assert inputs[test["data_logical"]], (
                        "Name=", str(test["name"]), "\ndata=", str(data), "\n",
                        str(inputs))
                if data in nooo:
                    assert not inputs[test["data_logical"]], (
                        "Name=", str(test["name"]), "\ndata=", str(data), "\n",
                        str(inputs))

        for data in test["invalid_data"]:
            inputs = GET_INPUTS()
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
    inputs = GET_INPUTS()
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
    # clean up
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
        inputs = GET_INPUTS()
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
