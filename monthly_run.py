#!/usr/bin/env python
"""
A script to split up long GEOS-Chem jobs on Earth0 into shorter runs.
This allows fitting in smaller queues and fairer access.

The jobs can call the next job in the sequence meaning you can submit in the
same way.
 - file name = monthly_run.py
There are also options that you can either pass as arguments or run a UI
if no arguments are passed.
see "$ monthly_run.py --help" for more information.
"""

from __future__ import absolute_import
from __future__ import print_function
import subprocess
import json
import os
import re
import sys
import shutil
import datetime
import calendar
from dateutil.relativedelta import relativedelta
import pytest


# Master debug switch for main driver
DEBUG = True

######
# To do
# Write complete test suit
# Migrate from class to dictionary when tests are complete
########



class GET_INPUTS:
    """
    A class containing all the variables needed
    Attributes:
        attribute:              default         - description
        job_name:               GEOS            - Name of the job to appear in qstat
        step:                   month           - Time of the split chunks
        queue_priority:         0               - Priority of the job (-1024 to 1023)
        queue_name:             run             - Name of the queue to submit too
        run_script_string:      yes             - Do you want to run the script immediately
        out_of_hours_string:    yes             - Do you only want to run evenings and weekends?
        wall_time:              "2:00:00"       - How long will a chunk take (overestimate)
        email_option:           "yes"           - Do you want an email upon completion?
        email_address:          "example@example.com" - Address to send emails to
        email_setting:          "e"             - Email on exit? google PBS email for more
        ncpu_need:              "16"            - Number of CPUs needed
        memory_need:            "16gb"          - Maximum memory you will need"
    """

    def __init__(self):

        script_location = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_location)
        user_settings_file = os.path.join(script_dir, 'settings.json')

        # set defaults
        self.job_name = "GEOS"
        self.step = "month"
        self.queue_priority = "0"
        self.queue_name = "run"
        self.run_script_string = "yes"
        self.out_of_hours_string = "no"
        self.wall_time = "2:00:00"
        self.email_option = "yes"
        self.email_address = "example@example.com"
        self.email_setting = "e"
        self.ncpu_need = "16"
        self.memory_need = "16gb"
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
        for key, val in _vars.items():
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
        _vars = {key:val for key, val in self.__dict__.items() if not built_in_function(key)}
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

    # Get the arguments from the command line or UI.
    inputs = get_arguments(inputs, debug=DEBUG)

    # Check all the inputs are valid.
    inputs = check_inputs(inputs, debug=DEBUG)

    # Check the start and end dates are compatible with the script.
    start_date, end_date = get_start_and_end_dates()

    # Calculate the list of months.
    times = list_of_times_to_run(start_date, end_date, inputs)

    # Make a backup of the input.geos file.
    backup_the_input_file()

    # Create the individual time input files.
    create_the_input_files(times)

    # Create the queue files.
    create_the_queue_files(times, inputs, debug=DEBUG)

    # Create the run script.
    create_the_run_script(times)

    # Send the script to the queue if wanted.
    run_the_script(inputs.run_script)

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

    queue_names = ['run', 'large']
    yess = ['yes', 'YES', 'Yes', 'Y', 'y']
    nooo = ['no', 'NO', 'No', 'N', 'n']
    steps = ["month", "week", "day"]

    assert (step in steps), str(
        "Unrecognised step size.",
        "try one of {steps}",
        ).format(steps=steps)

    assert (-1024 <= int(queue_priority) <= 1023), str(
        "Priority not between -1024 and 1023. Received {priority}"
        ).format(priority=queue_priority)

    assert (queue_name in queue_names), str(
        "Unrecognised queue type: {queue_name}"
        ).format(queue_name=queue_name)

    assert ((out_of_hours_string in yess) or (out_of_hours_string in nooo)), str(
        "Unrecognised option for out of hours.",
        "Try one of: {yess} / {nooo}",
        "The command given was {run_script_string}"
        ).format(yess=yess, nooo=nooo, run_script_string=run_script_string)

    assert (run_script_string in yess) or (run_script_string in nooo), str(
        "Unrecognised option for run the script on completion.",
        "Try one of: {yess} / {nooo}",
        "The command given was: {run_script_string}."
        ).format(yess=yess, nooo=nooo,
                 run_script_string=run_script_string)

    assert (email_option in yess) or (email_option in nooo), str(
        "Email option is neither yes or no.",
        "Please check the settings.",
        "Try one of: {yess} / {nooo}"
        ).format(yess=yess, nooo=nooo)

    # Create the logicals
    if run_script_string in yess:
        inputs["run_script"] = True
    elif run_script_string in nooo:
        inputs["run_script"] = False

    if out_of_hours_string in yess:
        inputs["out_of_hours"] = True
    elif out_of_hours_string in nooo:
        inputs["out_of_hours"] = False

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
    #with open('$HOME/.bashrc','a') as bashrc:
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
            elif arg.statswith("--ncpu-need="):
                inputs.ncpu_need = arg[13:].strip()
            elif arg.startswith("--memory-need="):
                inputs.memory_need = arg[14:].strip()
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
            print ("""Invalid argument {arg}
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
    ## TO DO
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
    ncpu_need = inputs.ncpu_need
    memory_need = inputs.memory_need
    step = inputs.step

    # Name the queue
    clear_screen()
    print('What name do you want in the queue?\n', \
    '(Will truncate to 9 characters).\n')
    input = str(raw_input('DEFAULT = ' + job_name + ' :\n'))
    if input:
        job_name = input

    # Specify the step size
    clear_screen()
    print('What time step size do you want?\n', \
        '(month recommended for 4x5, 2x25. week or day available).\n')
    input = str(raw_input('DEFAULT = ' + step + ' :\n'))
    if input:
        step = input

    # Give the job a priority
    clear_screen()
    print("What queue priority do you want? (Between -1024 and 1023).\n")
    input = str(raw_input('DEFAULT = ' + queue_priority + ' :\n'))
    if input:
        queue_priority = input

    # Choose the queue
    clear_screen()
    print("What queue do you want to go in?\n")
    input = str(raw_input('DEFAULT = ' + queue_name + ' :\n'))
    if input:
        queue_name = input

    # Check for out of hours run
    clear_screen()
    print("Do you only want to run jobs out of normal work hours?\n" \
        "(Monday to Friday 9am - 5pm)?\n")
    input = str(raw_input('Default = ' + out_of_hours_string + ' :\n'))
    if input:
        out_of_hours_string = input

    # Set the walltime for the run
    clear_screen()
    print("How long does it take to run a month (HH:MM:SS)?\n",
          "Be generous! if the time is too short your\n"
          "job will get deleted (Max = 48 hours)\n")
    input = str(raw_input('DEFAULT = ' + wall_time + ' :\n'))
    if input:
        wall_time = input

    # Gather number of CPUs required for the run
    clear_screen()
    print("How many CPUs does your run need?\n")
    input = str(raw_input('DEFAULT = ' + ncpu_need + ' :\n'))
    if input:
        ncpu_need = input

    # Set the memory requirements for the run
    clear_screen()
    print("How much memory does your run need?\n"
          "Lower amounts may increase priority.\n"
          "Example 4gb, 200mb, 200000kb.\n")
    input = str(raw_input('DEFAULT = ' + memory_need + ' :\n'))
    if input:
        memory_need = input

    # Run script check
    clear_screen()
    print("Do you want to run the script now?\n")
    input = str(raw_input('DEFAULT = ' + run_script_string + ' :\n'))
    if input:
        run_script_string = input

    clear_screen()

    # Update input variables
    inputs.job_name = job_name
    inputs.queue_name = queue_name
    inputs.queue_priority = queue_priority
    inputs.run_script_string = run_script_string
    inputs.out_of_hours_string = out_of_hours_string
    inputs.wall_time = wall_time
    inputs.ncpu_need = ncpu_need
    inputs.memory_need = memory_need
    inputs.step = step.lower()
    return inputs

def test_get_variables_from_cli():
    """
    Test that variables passed from the cli make it into the class.
    """
    #########
    ## To-do
    ########
    #
    # Write this test
    #
    ##########
    return


def run_the_script(run_script):
    """
    Call the run script with a subprocess command.
    """
    if run_script:
        subprocess.call(["bash", "run_geos.sh"])
    return


def clear_screen():
    """
    Clear the screen of the terminal for the UI.
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



def update_output_line(line, end_time):
    """
    Make sure we have a 3 in the end date in input.geos output menu
    INPUT:
        line - string pulled from the input file
        end_time - "YYYYMMDD"
    OUTPUT:
        line - string to write to the input file
    """

    # Get the name of the month
    _current_month_name = calendar.month_name[int(end_time[4:6])]
    _current_month_name = _current_month_name[0:3].upper()

    # Replace all instances of 3 with 0 so we only have the final day as 3
    line = line.replace('3', '0')

    # Get the position of the last day of simulations
    _current_day_of_month = int(end_time[6:8])
    _position = 26+_current_day_of_month

    if line[20:23] == _current_month_name:
        newline = line[:_position-1] + '3' + line[_position:]
    else:
        newline = line

    return newline


def is_current_year_a_leap_year():
    """ Check if current year is a leap year """
    # TODO
    return


def create_the_input_files(times, debug=False):
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

        time_input_file_location = os.path.join(_dir, (start_time+".input.geos"))

        new_input_geos = create_new_input_file(start_time, end_time, input_geos)

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

def create_new_input_file(start_time, end_time, input_file):
    """
    create a new input file based on the passed in open input file.
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
            newline = update_output_line(line, end_time)
        else:
            newline = line
        new_lines.append(newline)
    return new_lines



def create_the_queue_files(times, inputs, debug=DEBUG):
    """ """
    # Create local variables
    queue_name = inputs.queue_name
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    out_of_hours = inputs.out_of_hours
    wall_time = inputs.wall_time
    email = inputs.email
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    ncpu_need = inputs.ncpu_need
    memory_need = inputs.memory_need

    # Create folder queue files
    _dir = os.path.dirname("queue_files/")
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
          job_number=$(qsub -a 1810 queue_files/{start_time}.pbs)
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

        # TODO: REWORK THIS
        # CALCULCATE CHUNK SIZE AND MULTIPLIER:
        def gcd(x, y):
            while y != 0:
                z = y
                y = x % y
                x = z
            return x
        clean_mem_string = re.sub('[^0-9]', '', memory_need)
        chunk_multiplier = gcd(int(ncpu_need), int(clean_mem_string))
        chunk_ncpus = int(ncpu_need) / chunk_multiplier
        chunk_mem = int(clean_mem_string) / chunk_multiplier


        # Setup queue file string 
        queue_file_string = (
            """#!/bin/bash
#PBS -j oe
#PBS -V
#PBS -q {queue_name}
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N {job_name}
#PBS -r n
#PBS -l walltime={wall_time}
#PBS -l select={chunk_multiplier}:ncpus={chunk_ncpus}:mem={chunk_mem}G
#PBS -l place=vscatter:shared
#
#PBS -o queue_output/{start_time}.output
#PBS -e queue_output/{start_time}.error
#
# Set priority.
#PBS -p {queue_priority}


{email_string}


cd $PBS_O_WORKDIR

# Make sure the required dirs exists
mkdir -p queue_output
mkdir -p logs
mkdir -p queue_files

# Set environment variables
#
set -x
#
#
export OMP_WAIT_POLICY=active
export OMP_DYNAMIC=false
export OMP_PROC_BIND=true

export OMP_NUM_THREADS={omp_num_threads}
export F_UFMTENDIAN=big
export MPSTZ=1024M
export KMP_STACKSIZE=100000000
export KMP_LIBRARY=turnaround
export FORT_BUFFERED=true
ulimit -s 200000000


{out_of_hours_string}

# Change to the directory that the command was issued from
echo running in $PBS_O_WORKDIR > logs/log.log
echo starting on $(date) >> logs/log.log

# Create the exit file
echo qdel $job_number > exit_geos.sh
chmod 775 exit_geos.sh


rm -f input.geos
ln -s input_files/{start_time}.input.geos input.geos
/opt/hpe/hpc/mpt/mpt-2.16/bin/omplace ./geos > logs/{start_time}.geos.log

# Prepend the files with the date
mv ctm.bpch {start_time}.ctm.bpch
mv HEMCO.log logs/{start_time}.HEMCO.log

# Only submit the next month if GEOS-Chem completed correctly
last_line = "$(tail -n1 {start_time}.geos.log)"
complete_last_line = "**************   E N D   O F   G E O S -- C H E M   **************"

if [ $last_line = $complete_last_line]; then
   job_number=$(qsub queue_files/{end_time}.pbs)
   echo $job_number
fi
"""
        )

        # Add all the variables to the string
        queue_file_string = queue_file_string.format(
            queue_name=queue_name,
            job_name=(job_name + start_time)[:14], # job name can only be 15 characters
            start_time=start_time,
            wall_time=wall_time,
            ncpu_need=ncpu_need,
            memory_need=memory_need,
            queue_priority=queue_priority,
            email_string=email_string,
            out_of_hours_string=out_of_hours_string,
            end_time=end_time,
            chunk_multiplier=chunk_multiplier,
            chunk_ncpus=chunk_ncpus,
            chunk_mem=chunk_mem,
            omp_num_threads=ncpu_need
            )

        queue_file_location = os.path.join(_dir, (start_time + ".pbs"))
        queue_file = open(queue_file_location, 'wb')
        queue_file.write(queue_file_string)
        # If this is the final month then run an extra command
        if time == times[-1]:
            run_completion_script()
        queue_file.close()
        start_time = time
    return


def create_the_run_script(months):
    """
    Create the script that can set off the run jobs for rerunning or manual
    submission.
    Input: months
    Output: 'run_geos.sh'
    """
    run_script = open('run_geos.sh', 'w')
    run_script_string = ("""
#!/bin/bash
qsub queue_files/{month}.pbs
     """).format(month=months[0])
    run_script.write(run_script_string)
    run_script.close()
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
        "valid_data": ["run"],
        "invalid_data": ["bob"]
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
                    assert inputs[test["data_logical"]], ( \
                        "Name=", str(test["name"]), "\ndata=", str(data), "\n",
                        str(inputs))
                if data in nooo:
                    assert not inputs[test["data_logical"]], ( \
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
    for step in ["month", "week", "day"]:
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
    dayly = {"step": "day",
             "start_time": "20000101",
             "end_time": "20000106",
             "expected_output": ["20000101", "20000102", "20000103",
                                 "20000104", "20000105", "20000106"]
            }

    tests = [monthly, leap_year, dayly]

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
        assert test["lineout"] == update_output_line(test["linein"], test["end_time"])

    return
