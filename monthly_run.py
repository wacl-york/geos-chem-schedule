#!/usr/local/anaconda/bin/python
"""
A script to split up long GEOS_Chem jobs on Earth0 into shorter runs.
This allows fitting in smaller queues and fairer access.

The jobs can call the next job in the sequence meaning you can submit in the
same way.

There are also options that you can either pass as arguments or run a UI
if no arguments are passed.
see "$ monthly_run.py --help" for more information.
"""

# file name = monthly_run.py

#############
# If you want to edit the default settings, then after the first run,
# The script will create a settings.json that you can change.
#############


from __future__ import absolute_import
import subprocess
import json
import os
import sys
import shutil
import datetime
import calendar
from dateutil.relativedelta import relativedelta


DEBUG = True
class GET_INPUTS:
    """
    A dictionary containing all the variables needed
    """
    def __init__(self):

        script_location = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_location)
        user_settings_file = os.path.join(script_dir, 'settings.json')

        if not os.path.exists(user_settings_file):

            default = {}


            default["job_name"]             = "GEOS"    # Name of the job that appears in qstat
            default["step"]                 = "month"    # Name of the job that appears in qstat
            default["queue_priority"]       = "0"       # Priority of the job
            default["queue_name"]           = "run"     # Name of the queue to submit too
            default["run_script_string"]    = "yes"     # Do you want to run the script streight away?
            default["out_of_hours_string"]  = "no"      # Do you only want to run evenings and weekends?
            default["wall_time"]            = "2:00:00"# How long will a month take at most?
            default["email_option"]         = "yes"     # Do you want an email sending upon completion?
            default["email_address"]        = "example@example.com"
            default["email_setting"]        = "e"       # Do you want an email on exit(e) or othe settings - see PBS email.
            default["memory_need"]          = "10gb"    # How much memory do you need? 

            settings_file = open(user_settings_file, 'w')
            json.dump(default, settings_file, sort_keys=True, indent=4)
            settings_file.close()

        settings_file = open(user_settings_file, 'r')
        options = json.load(settings_file)

        self.__dict__.update(options)

        return


def run_completion_script():
    """
    Run a script when the final month finishes. This could be a cleanup script
    or a post processing script.
    """
    return


# Nothing below here should need changing, but feel free to look.



def main( debug=DEBUG ):


    # Get the default inputs as a class
    inputs = GET_INPUTS()
 
    # Get the arguments from the comand line or UI.
    inputs = get_arguments( inputs, debug=DEBUG)
 
    # Check all the inputs are valid.
    inputs = check_inputs(inputs, debug=DEBUG)
 
    # Check the start and end dates are compatible with the script.
    start_date, end_date = get_start_and_end_dates()
 
    # Calculate the list of months.
    times = list_of_times_to_run( start_date, end_date, inputs )
 
    # Make a backup of the input.geos file.
    backup_the_input_file()
    
    # Create the individual time input files.
    create_the_input_files(times)
   
    # Create the queue files.
    create_the_queue_files(times, inputs, debug=DEBUG )
 
    # Create the run script.
    create_the_run_script(times)
 
    # Send the script to the queue if wanted.
    run_the_script(inputs.run_script)
 
    return;

def check_inputs(inputs, debug=False):
    """
    Make sure all the inputs make sense
    """

    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    email_option = inputs.email_option
    wall_time = inputs.wall_time
    step = inputs.step
    
    queue_names = ['core16', 'core32', 'core64', 'batch', 'run']
    yes         = ['yes', 'YES', 'Yes', 'Y', 'y'] 
    no          = ['no', 'NO', 'No', 'N', 'n'] 
    steps = ["month", "week", "day"]

    assert (step in steps), (
            "Unrecognised step size.",
            "try one of {steps}",
            ).format( steps=steps )

 
 
    assert (-1024 <= int(queue_priority) <= 1023),(
        "Priority not between -1024 and 1023. Recived {priority}"
                ).format(priority=queue_priority) 
     
    assert (queue_name in queue_names),(
        "Unrecognised queue type: {queue_name}"
                ).format(queue_name=queue_name)
 
    
    assert ((out_of_hours_string in yes) or (out_of_hours_string in no)),(
    "Unrecognised option for out of hours.",
    "Try one of: {yes} / {no}",
    "The command given was {run_script_string}"
            ).format(yes=yes, no=no, run_script_string=run_script_string)
 
    assert (run_script_string in yes) or (run_script_string in no), (
            "Unrecognised option for run the script on completion.",
            "Try one of: {yes} / {no}", 
            "The command given was: {run_script_string}."
                    ).format(yes=yes, no=no, 
                            run_script_string=run_script_string)
 
    assert (email_option in yes) or (email_option in no),(
            "Email option is neither yes or no.",
            "Please check the settings.",
            "Try one of: {yes} / {no}"
                    ).format(yes=yes, no=no)
 
 
    # Create the logicals
    if (run_script_string in yes):
       run_script = True
    elif (run_script_string in no):
       run_script = False
 
    if (out_of_hours_string in yes):
       out_of_hours = True
    elif (out_of_hours_string in no):
       out_of_hours = False
 
    if (email_option in yes):
       email = True
    elif (email_option in no):
       email = False
    
 
    if debug: print str(out_of_hours)
 
    inputs.run_script = run_script
    inputs.out_of_hours = out_of_hours
    inputs.email = email
 
    return inputs;


def backup_the_input_file():
    """
    Save a copy of the origional input file
    """

    input_file = "input.geos"
    backup_input_file = "input.geos.orig"

    if not os.path.isfile( backup_input_file ):
       shutil.copyfile( input_file, backup_input_file )

    return;

def setup_script():
    """
    Creates a symbolic link allowing the use to run "monthly_run" from any folder"
    """

    print "\n"
    print "Monthly run setup complete. Change your default settings in settings.json"
    print "To run the script from anywhere with the monthly_run command,"
    print "copy the following code into your terminal. \n"

    script_location = os.path.realpath(__file__)
    # make sure the script is excecutable
    print "chmod 755 {script}".format(script=script_location)
    # Make sure there is a ~/bin file
    print "mkdir -p $HOME/bin"
    # Create a symlink from the file to the bin
    print "ln -s {script} $HOME/bin/monthly_run".format(script=script_location)
    # Make sure the ~/bin is in the bashrc
    #with open('$HOME/.bashrc','a') as bashrc:
    #        bashrc.write('## Written by monthly_run')
    #        bashrc.write('export PATH=$PATH:$HOME/bin')
    print 'echo "## Written by monthly_run" >> $HOME/.bashrc'
    print 'echo "export PATH=\$PATH:\$HOME/bin" >> $HOME/.bashrc'
    # source the bashrc
    print "source $HOME/.bashrc"
    print 
    sys.exit()


def get_arguments(inputs, debug=DEBUG):
    """
    Get the arguments supplied from command line
    """

   # If there are no arguments then run the gui.
    if len(sys.argv)>1:
      for arg in sys.argv:
         if "monthly-run" in arg: continue
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
            print ("""
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
                     Try --help for more info."""
                     ).format(arg=arg)
    else:
        inputs = get_variables_from_cli(inputs)
    return inputs

def get_variables_from_cli(inputs):
    """
    Get the variables needed from a UI
    """

    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    queue_name  = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    wall_time = inputs.wall_time
    memory_need = inputs.memory_need
    step = inputs.step

    # Name the queue
    clear_screen()
    print 'What name do you want in the queue?',
    print '(Will truncate to 9 charicters).'
    input = str(raw_input( 'DEFAULT = ' + job_name + ' :\n'))
    if (len(input) != 0): job_name = input

    # Specify the step size
    clear_screen()
    print 'What time step size do you want?',
    print '(month recomended for 4x5, 2x25. week or day available).'
    input = str(raw_input( 'DEFAULT = ' + step + ' :\n'))
    if (len(input) != 0): step = input

    # Give the job a priority
    clear_screen()
    print "What queue priority do you want? (Between -1024 and 1023)."
    input = str(raw_input( 'DEFAULT = ' + queue_priority + ' :\n'))
    if (len(input) != 0): queue_priority = input

    # Choose the queue
    clear_screen()
    print "What queue do you want to go in?" 
    input = str(raw_input( 'DEFAULT = ' + queue_name + ' :\n'))
    if (len(input) != 0): queue_name = input

    # Check for out of hours run
    clear_screen()
    print "Do you only want to run jobs out of normal work hours?"
    print "(Monday to Friday 9am - 5pm)?"
    input = str(raw_input('Default = ' + out_of_hours_string + ' :\n'))
    if (len(input) != 0): out_of_hours_string = input

    # Set the walltime for the run   
    clear_screen()
    print "How long does it take to run a month (HH:MM:SS)?"
    print "Be generous! if the time is too short your"
    print "job will get deleted (Max = 48 hours)"
    input = str(raw_input( 'DEFAULT = ' + wall_time + ' :\n'))
    if (len(input) != 0): wall_time = input

    # Set the memory requirements for the run 
    clear_screen()
    print "How much memory does your run need?"
    print "Lower amounts may increase priority."
    print "Example 4gb, 200mb, 200000kb."
    input = str(raw_input( 'DEFAULT = ' + memory_need + ' :\n'))
    if (len(input) != 0): memory_need = input


    # Run script check
    clear_screen()
    print "Do you want to run the script now?"
    input = str(raw_input( 'DEFAULT = ' + run_script_string + ' :\n'))
    if (len(input) != 0): run_script_string = input

    clear_screen()   


    inputs.job_name = job_name
    inputs.queue_name = queue_name
    inputs.queue_priority = queue_priority
    inputs.run_script_string = run_script_string
    inputs.out_of_hours_string = out_of_hours_string
    inputs.wall_time = wall_time
    inputs.memory_need = memory_need
    inputs.step = step.lower()
    return inputs

def run_the_script(run_script):
   if run_script:
      subprocess.call(["bash", "run_geos.sh"])
   return;

def clear_screen():
   os.system('cls' if os.name == 'nt' else 'clear')
   return

def get_start_and_end_dates():
    """
    Get the start date and end date from input.geos
    """
    input_geos = open( 'input.geos', 'r' )
    for line in input_geos:
       if line.startswith("Start YYYYMMDD, HHMMSS  :"):
          start_date = line[26:34]
          # Confirm the run starts on the first of the month
          #start_day=str(line[32:34])
          #if not start_day == "01":
          #   sys.exit( """The month does not start on the first.
          #           recived {start_day}""".format(start_day=start_day))
       if line.startswith("End   YYYYMMDD, HHMMSS  :"):
          end_date = line[26:34]
#          end_day  = str(line[32:34])
          #if not end_day == "01":
          #   sys.exit("""The month does not end on the first.
          #           Recived {end_day}""".format(end_day=end_day))

    print "Start time = {start_date}".format(start_date=start_date)
    print "End time = {end_date}".format(end_date=end_date)
    input_geos.close()

    return start_date, end_date;

def list_of_times_to_run( start_time, end_time, inputs, debug=False):
    """
    Create a list of start times and the end time of the run
    """

    step=inputs.step

    ###### TO-DO
    ######
    # This is the main file i need to change to get the list of times to run
    # To get most compatability I will need to change the output formats of the
    # fils from YYYYMM to YYYYMMDD so that I can do that
    # I also need to add some checks for the input.geos so that all the output
    # dates have a 3 on them.
    #######


    def datetime_2_YYYYMMDD(_my_datetime):
        return _my_datetime.strftime("%Y%m%d")


    if step=="month":
        time_delta = relativedelta(months=1)
    elif step=="week":
        time_delta = relativedelta(weeks=1)
    elif step=="day":
        time_delta = relativedelta(days=1)


    start_datetime = datetime.datetime.strptime(start_time, "%Y%m%d")
    end_datetime = datetime.datetime.strptime(end_time, "%Y%m%d")

    _timestamp = (start_datetime)

    times = [ datetime_2_YYYYMMDD(_timestamp) ]
    while _timestamp < end_datetime:
        _timestamp = _timestamp + time_delta
        times.append( datetime_2_YYYYMMDD(_timestamp) )


    return times;

def update_output_line( line, end_time ):
    """
    Make sure we have a 3 in the end date in input.geos output menu
    """

#    _current_month_name = end_time.strf(time("%B"))
    _current_month_name = calendar.month_name[int(end_time[4:6])]
    _current_month_name = _current_month_name[0:3].upper()


    _current_day_of_month = int(end_time[6:8])
    _position_in_string = 26+_current_day_of_month

    _line_start = line[:_position_in_string-1]
    _line_end = line[_position_in_string:]
    
    if line[20:23] == _current_month_name:
        line = _line_start +'3' + _line_end
    else:
        line = _line_start + '0' + _line_end

    return line


def create_the_input_files(times, debug=False):
    # create folder input files 
    dir = os.path.dirname("input_files/")
    if not os.path.exists(dir):
        os.makedirs(dir)     


    # modify the input files to have the correct start times
    # Also make sure they end on a 3
    for time in times:
        end_time = time
        if (time == times[0]):
            start_time = time
            continue
      
        if debug:
             print ("""start time = {start_time}
                 End time = {end_time}"""
                ).format(start_time=start_time, end_time=end_time)
   
        input_geos = open( 'input.geos', 'r' )
        time_input_file_location = os.path.join(dir, (start_time+".input.geos"))
        output_file = open(time_input_file_location, 'w')
      
        if debug:
             print ("writing to file {filename}"
                 ).format(filename=time_input_file_location)


        for line in input_geos:

            if line.startswith("Start YYYYMMDD, HHMMSS  :"):
                newline = line[:26] + str(start_time) + line[34:] 
                output_file.write(newline)
                # Confirm the run starts on the first of the time
            elif line.startswith("End   YYYYMMDD, HHMMSS  :"):
                newline = line[:26] + str(end_time) + line[34:] 
                output_file.write(newline)
         
         # Force CSPEC on
            elif line.startswith("Read and save CSPEC_FULL:"):
                newline = line[:26] + 'T \n' 
                output_file.write(newline)
            # Make sure write at end on a 3
            elif line.startswith("Schedule output for"):
                newline = update_output_line( line, end_time )
                output_file.write(newline)


            else: 
                newline = line
                output_file.write(newline)
        output_file.close()
        start_time = time
        input_geos.close()
    return;

def create_the_queue_files(times, inputs, debug=DEBUG ):

    # Create local varialbes
    queue_name = inputs.queue_name
    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    out_of_hours = inputs.out_of_hours
    wall_time = inputs.wall_time
    email = inputs.email
    email_address = inputs.email_address
    email_setting = inputs.email_setting
    memory_need   = inputs.memory_need

    ## create folder queue files 
    dir = os.path.dirname("queue_files/")
    if not os.path.exists(dir):
        os.makedirs(dir)     

    # modify the input files to have the correct start months
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
          echo qdel $job_numner > exit_geos.sh                                   
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
#PBS -l mem={memory_need}
#PBS -l nodes=1:ppn=16
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

# set enviroment variables
#
set -x
#
#
export OMP_WAIT_POLICY=active
export OMP_DYNAMIC=false
export OMP_PROC_BIND=true

export OMP_NUM_THREADS=16
export F_UFMTENDIAN=big
export MPSTZ=1024M
export KMP_STACKSIZE=100000000
export KMP_LIBRARY=turnaround
export FORT_BUFFERED=true
ulimit -s 200000000


{out_of_hours_string}

#change to the directory that the command was issued from
echo running in $PBS_O_WORKDIR > logs/log.log
echo starting on $(date) >> logs/log.log

# Create the exit file
echo qdel $job_number > exit_geos.sh
chmod 775 exit_geos.sh


rm -f input.geos
ln -s input_files/{start_time}.input.geos input.geos
/opt/sgi/mpt/mpt-2.09/bin/omplace ./geos > logs/{start_time}.geos.log

# Prepend the files with the date
mv ctm.bpch {start_time}.ctm.bpch
mv HEMCO.log logs/{start_time}.HEMCO.log

# Only submit the next month if GEOSCHEM completed correctly
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
            job_name=(job_name + start_time)[:14], # job name can only be 15 charicters
            start_time=start_time,
            wall_time=wall_time,
            memory_need=memory_need,
            queue_priority=queue_priority,
            email_string=email_string,
            out_of_hours_string=out_of_hours_string,
            end_time=end_time
            )

        queue_file_location = os.path.join(dir, (start_time + ".pbs"))
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


if __name__ == '__main__':
    main(debug=DEBUG)
