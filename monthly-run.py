#!/usr/bin/python

# file name = monthly-run.py

# Splits up GEOSChem runs into smaller jobs, to be fairer on the queues.


DEBUG = False

# Change the defaults here

class GET_INPUTS:
    def __init__(self):
        self.job_name           = "GEOS"    # Name of the job that appears in qstat
        self.queue_priority     = "0"       # Priority of the job
        self.queue_name         = "run"     # Name of the queue to submit too
        self.run_script_string  = "yes"     # Do you want to run the script streight away?
        self.out_of_hours_string= "yes"      # Do you only want to run evenings and weekends?
        self.wall_time          = "2:00:00"# How long will a month take at most?
        self.email_option       = "yes"     # Do you want an email sending upon completion?
        self.email_address      = "bn506+PBS@york.ac.uk"
        self.email_setting      = "e"       # Do you want an email on exit(e) or othe settings - see PBS email.
        self.memory_need        = "10gb"    # How much memory do you need? 


# If you have a script you would like to run upon completion, such as analyse some results, or turn results into a different format, then insert it in the run_completion_script function.
def run_completion_script(job_name):
    import subprocess
    #   subprocess.call( 'A script' )
    return


# Nothing below here should need changing, but feel free to look.


import os
import sys
import math
import shutil
import subprocess

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
    times = list_of_months_to_run( start_date, end_date )
 
    # Make a backup of the input.geos file.
    backup_the_input_file()
    
    # Create the individual month input files.
    create_the_input_files(times)
   
    # Create the queue files.
    create_the_queue_files(times, inputs, debug=DEBUG )
 
    # Create the run script.
    create_the_run_script(times)
 
    # Send the script to the queue if wanted.
    run_the_script(inputs.run_script)
 
    return;

def check_inputs(inputs, debug=False):

    job_name = inputs.job_name
    queue_priority = inputs.queue_priority
    queue_name = inputs.queue_name
    run_script_string = inputs.run_script_string
    out_of_hours_string = inputs.out_of_hours_string
    email_option = inputs.email_option
    wall_time = inputs.wall_time
    
    queue_names = ['core16', 'core32', 'core64', 'batch', 'run']
    yes         = ['yes', 'YES', 'Yes', 'Y', 'y'] 
    no          = ['no', 'NO', 'No', 'N', 'n'] 
 
 
    assert (-1024 <= int(queue_priority) <= 1023), "Priority not within bounds of -1024 and 1023, recived " + str(queue_priority) 
 
    assert (queue_name in queue_names), "Unrecognised queue type: " + str(queue_name)
 
    
    assert ((out_of_hours_string in yes) or (out_of_hours_string in no)), "Unrecognised option for out of hours. Try one of: " + str(yes) + str(no)+'. The command given was "' + run_script_string +'".'
 
    assert (run_script_string in yes) or (run_script_string in no), "Unrecognised option for run the script on completion. Try one of: " + str(yes) + str(no) +'. The command given was "' + run_script_string + '".'
 
    assert (email_option in yes) or (email_option in no), "Email option is neither yes or no, please check the settings. Try one of: " + str(yes) + str(no)
 
 
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

   input_file = "input.geos"
   backup_input_file = "input.geos.orig"

   if not os.path.isfile( backup_input_file ):
      shutil.copyfile( input_file, backup_input_file )
   
   return;


def get_arguments(inputs, debug=DEBUG):

   job_name = inputs.job_name
   queue_priority = inputs.queue_priority
   queue_name  = inputs.queue_name
   run_script_string = inputs.run_script_string
   out_of_hours_string = inputs.out_of_hours_string
   wall_time = inputs.wall_time
   memory_need = inputs.memory_need



   # If there are no arguments then run the gui.
   if len(sys.argv)>1:
      for arg in sys.argv:
         if "monthly-run" in arg: continue
         if arg.startswith("--job-name="):
            job_name = arg[11:]
         elif arg.startswith("--queue-name="):
            queue_name = arg[13:]
         elif arg.startswith("--queue-priority="):
            queue_priority = arg[17:]
         elif arg.startswith("--submit="):
            run_script_string = arg[9:]
         elif arg.startswith("--out-of-hours="):
            out_of_hours_string = arg[15:] 
         elif arg.startswith("--wall-time="):
            wall_time = arg[12:] 
         elif arg.startswith("--memory-need="):
            wall_time = arg[14:] 
         elif arg.startswith("--help"):
            print "monthly-run.py\n"

            print "For UI run without arguments\n"
            print "Arguments are:\n"
            print "--job-name=\n"
            print "--queue-name=\n"
            print "--queue-priority=\n"
            print "--submit=\n"
            print "--out-of-hours=\n"
            print "--wall-time=\n"
            print "--memory-need=\n"
            print "e.g. to set the queue name to 'bob' write --queue-name=bob \n"
         else:
             print "Invalid argument "+ arg +"\nTry --help for more info.\n"

   else:
   
      # Name the queue
      clear_screen()
      print "What name do you want in the queue? (Will truncate to 9 charicters)."
      input = str(raw_input( 'DEFAULT = ' + job_name + ' :\n'))
      if (len(input) != 0): job_name = input
   
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
      print 'Do you only want to run jobs out of normal work hours (Monday to Friday 9am - 5pm)?'
      input = str(raw_input('Default = ' + out_of_hours_string + ' :\n'))
      if (len(input) != 0): out_of_hours_string = input

      # Set the walltime for the run   
      clear_screen()
      print "How long does it take to run a month (HH:MM:SS)? \n Be generous, if the time is too short, your job will get deleted (Max = 48 hours)"
      input = str(raw_input( 'DEFAULT = ' + wall_time + ' :\n'))
      if (len(input) != 0): wall_time = input

      # Set the memory requirements for the run 
      clear_screen()
      print "How much memory does your run need. Lower amounts may increase priority.\n Example 4gb, 200mb,200000kb."
      input = str(raw_input( 'DEFAULT = ' + memory_need + ' :\n'))
      if (len(input) != 0): memory_need = input


      # Run script check
      clear_screen()
      print "Do you want to run the script now?"
      input = str(raw_input( 'DEFAULT = ' + run_script_string + ' :\n'))
      if (len(input) != 0): run_script_string = input
      
      clear_screen()

   # Only take the first 9 charicters from the job name
   job_name = job_name[:9]


   # Strip all whitespace
   job_name           = job_name.strip()
   queue_name         = queue_name.strip()
   queue_priority     = queue_priority.strip()     
   run_script_string  = run_script_string.strip()
   wall_time          = wall_time.strip()
   out_of_hours_string= out_of_hours_string.strip()
   memory_need        = memory_need.strip()

   if debug:
      print "job name         = " + str(job_name[:9])
      print "queue name       = " + str(queue_name)
      print "queue priority   = " + str(queue_priority )
      print "run script       = " + str(run_script_string)
      print "wall time        = " + str(wall_time)
      print "memory_need      = " + str(memory_need)
      print "out of hours     = " + str(out_of_hours_string)

   inputs.job_name = job_name
   inputs.queue_priority = queue_priority
   inputs.run_script_string = run_script_string
   inputs.out_of_hours_string = out_of_hours_string
   inputs.wall_time = wall_time
   inputs.memory_need = memory_need

   return inputs;

def run_the_script(run_script):
   if run_script:
      subprocess.call(["bash", "run_geos.sh"])
   return;

def clear_screen():
   os.system('cls' if os.name == 'nt' else 'clear')
   return

def get_start_and_end_dates():
   input_geos = open( 'input.geos', 'r' )
   for line in input_geos:
      if line.startswith("Start YYYYMMDD, HHMMSS  :"):
         start_date = line[26:32]
         # Confirm the run starts on the first of the month
         if str(line[32:34]) != "01":
            sys.exit( "The month does not start on the first, recived " + line[33:34] )
      if line.startswith("End   YYYYMMDD, HHMMSS  :"):
         end_date = line[26:32]
         if str(line[32:34]) != "01":
            sys.exit( "The month does not end on the first, recived " + line[33:34] )

   print "Start date = " + str(start_date)
   print "End date = " + str(end_date)
   input_geos.close()

   return start_date, end_date;

def list_of_months_to_run( start_date, end_date, debug=False):
   start_year = int(start_date[0:4])
   start_month = int(start_date[4:6])
   end_year = int(end_date[0:4])
   end_month = int(end_date[4:6])

   months = []
   number_months = []
   counter = 0

   start_number_month = start_year*12 + start_month - 1
   end_number_month = end_year*12 + end_month - 1

   total_number_of_months = end_number_month - start_number_month
   
   number_month = start_number_month
   while counter <= total_number_of_months:
      number_months.append(number_month)
      number_month = number_month + 1
      counter = counter + 1
      
   for item in number_months:
      months.append( str(int(math.floor(item/12))) + str((item%12)+1).zfill(2) )  

   for month in months:
      if debug: print month

   return months;


def create_the_input_files(months, debug=False):

# create folder input files 
   dir = os.path.dirname("input_files/")
   if not os.path.exists(dir):
      os.makedirs(dir)     


# modify the input files to have the correct start months
   for month in months:
      end_time = month
      if (month == months[0]):
         start_time = month
         continue
      
      if debug:
         print "start time = " + str(start_time) +" End time =  " + str(end_time)
   
      input_geos = open( 'input.geos', 'r' )
      month_input_file_location = os.path.join(dir, (start_time+".input.geos"))
#      shutil.copy( "input.geos" , month_input_file_location)
      output_file = open(month_input_file_location, 'w')
      
      if debug:
         print "writing to file " + month_input_file_location      


      for line in input_geos:
         if line.startswith("Start YYYYMMDD, HHMMSS  :"):
            newline = line[:26] + str(start_time) + line[32:] 
            output_file.write(newline)
            # Confirm the run starts on the first of the month
         elif line.startswith("End   YYYYMMDD, HHMMSS  :"):
            newline = line[:26] + str(end_time) + line[32:] 
            output_file.write(newline)
         
         # Force CSPEC on
         elif line.startswith("Read and save CSPEC_FULL:"):
            newline = line[:26] + 'T \n' 
            output_file.write(newline)
         else: 
            newline = line
            output_file.write(newline)
      output_file.close()
      start_time = month
      input_geos.close()
   return;

def create_the_queue_files(months, inputs, debug=DEBUG ):

   queue_name = inputs.queue_name
   job_name = inputs.job_name
   queue_priority = inputs.queue_priority
   out_of_hours = inputs.out_of_hours
   wall_time = inputs.wall_time
   email = inputs.email
   email_address = inputs.email_address
   email_setting = inputs.email_setting
   memory_need   = inputs.memory_need
# create folder queue files 
   dir = os.path.dirname("queue_files/")
   if not os.path.exists(dir):
      os.makedirs(dir)     
   pbs_output_dir = os.path.dirname("queue_output/")
   if not os.path.exists(pbs_output_dir):
      os.makedirs(pbs_output_dir)   

# modify the input files to have the correct start months
   for month in months:
      end_time = month
      if month == months[0]:
         start_time = month
         continue
      queue_file_location = os.path.join(dir , (start_time + ".pbs"))
      queue_file = open( queue_file_location, 'wb' )
# set PBS variables
      queue_file.write(
"""#!/bin/bash
#PBS -j oe
#PBS -V
#PBS -q """ + str(queue_name) + """
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N """ + job_name + start_time + """
#PBS -r n
#PBS -l walltime=""" + wall_time + """
#PBS -l mem=""" + memory_need + """
#PBS -l nodes=1:ppn=16
#
#PBS -o queue_output/""" + start_time + """.output
#PBS -e queue_output/""" + start_time + """.error
#
# Set priority.
#PBS -p """ + queue_priority + """
""")

      # if the final month and we want to email:
      if month == months[-1]:
         if email:
            queue_file.write("""
#PBS -m """ + email_setting + """
#PBS -M """ + email_address + """
""")




# set enviroment variables      
      queue_file.write("""
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

""")

      if out_of_hours:
         queue_file.write("""
export out_of_hours=true
""")
      else:
         queue_file.write("""
export out_of_hours=false
""")

      queue_file.write("""


#change to the directory that the command was issued from
cd $PBS_O_WORKDIR
echo running in $PBS_O_WORKDIR > log.log
echo starting on $(date) >> log.log

# If the out of hours overide is not set in bash then set it here
if [ -z $out_of_hours_overide ]; then
   export out_of_hours_overide=false
   echo out_of_hours_overide is not set in bashrc >> log.log
fi


if ! ( $out_of_hours_overide ); then
   if $out_of_hours ; then 
      if [ $(date +%u) -lt 6 ]  && [ $(date +%H) -gt 8 ] && [ $(date +%H) -lt 17 ] ; then
         job_number=$(qsub -a 1810 queue_files/""" + str(start_time)+""".pbs)
         echo $job_number
         echo qdel $job_numner > exit_geos.sh
         echo "Tried running in work hours but we don't want to. Will try again at 1800. The time we attempted to run was:">>log.log
         echo $(date)>>log.log
         exit 1
      fi
   fi
fi

# Create the exit file
echo qdel $job_number > exit_geos.sh
chmod 775 exit_geos.sh

# Make sure using the spinup input file

rm -f input.geos
ln -s input_files/""" + str(start_time) +""".input.geos input.geos
/opt/sgi/mpt/mpt-2.09/bin/omplace ./geos > """+ str(start_time) + """.geos.log
mv ctm.bpch """+str(start_time)+""".ctm.bpch

# Only submit the next month if GEOSCHEM completed correctly
last_line = tail -n1 """ + str(start_time) + """.geos.log 
complete_last_line = **************   E N D   O F   G E O S -- C H E M   **************

if [ $last_line = $complete_last_line]; then
   job_number=$(qsub queue_files/"""+str(end_time)+""".pbs)
   echo $job_number
fi
"""
   )
   
      # If this is the final month then run an extra command
      if month == months[-1]:
         run_completion_script(job_name)

      queue_file.close()
      start_time = month
   return;


def create_the_run_script(months):
   run_script = open('run_geos.sh','w')
   run_script.write(
   """#!/bin/bash
qsub queue_files/"""+str(months[0])+""".pbs
""")
   run_script.close()
   return;




main( debug=DEBUG )
