#!/usr/bin/python

# monthly-run.py

import os
import sys
import math
import shutil
import subprocess
# Inputs

# Automaticaly submit the generated run script

# If cmnd line arg then run silently, else ask

debug=True

#Defaults

job_name             = "GEOS"          # Name of the job that appears in qstat
queue_priority       = "-1000"         # Priority of the job
queue_name           = "batch"         # Name of the queue to submit too
run_script_string    = "yes"           # Do you want to auto submit the job with this script?
out_of_hours_string  = "no"            # Do you only want to run out of normal work hours? 

def main( job_name, queue_priority, queue_name, run_script_string, out_of_hours_string, debug ):

   # Get the arguments from the comand line or UI.
   job_name, queue_priority, queue_name, run_script_string, out_of_hours_string = get_arguments( job_name, queue_priority, queue_name, run_script_string, out_of_hours_string, debug)

   # Check all the inputs are valid.
   run_script, out_of_hours = check_inputs(job_name, queue_priority, queue_name, run_script_string, out_of_hours_string)

   # Check the start and end dates are compatible with the script.
   start_date, end_date = get_start_and_end_dates()

   # Calculate the list of months.
   months = list_of_months_to_run( start_date, end_date )
   
   # Create the individual month input files.
   create_the_input_files(months)
  
   # Create the queue files.
   create_the_queue_files(months, queue_name, job_name, queue_priority, out_of_hours)

   # Create the run script.
   create_the_run_script(months)

   # Send the script to the queue if wanted.
   run_the_script(run_script)

   return;

def check_inputs(job_name, queue_priority, queue_name, run_script_string, out_of_hours_string, debug=True):
   
   queue_names = ['core16', 'core32', 'core64', 'batch', 'run']
   yes         = ['yes', 'YES', 'Yes', 'Y', 'y'] 
   no          = ['no', 'NO', 'No', 'N', 'n'] 

   assert (len(job_name) <= 9), "Job name is too long," + str(len(job_name)) + " charicters long"

   assert (-1024 <= int(queue_priority) <= 1023), "Priority not within bounds of -1024 and 1023, recived " + str(queue_priority) 

   assert (queue_name in queue_names), "Unrecognised queue type: " + str(queue_name)

   
   assert ((out_of_hours_string in yes) or (out_of_hours_string in no)), "Unrecognised option for out of hours. Try one of: " + str(yes_no)

   assert (run_script_string in yes) or (run_script_string in no), "Unrecognised option for run the script on completion. Try one of: " + str(yes) + str(no)


   # Create the logicals
   if (run_script_string in yes):
      run_script = True
   elif (run_script_string in no):
      run_script = False

   if (out_of_hours_string in yes):
      out_of_hours = True
   elif (out_of_hours_string in no):
      out_of_hours = False

   if debug: print str(out_of_hours)

   return run_script, out_of_hours;

def get_arguments(job_name, queue_priority, queue_name, run_script_string, out_of_hours_string, debug):


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
         elif arg.startswith("--help"):
            print "monthly-run.py\n"

            print "For UI run without arguments\n"
            print "Arguments are:\n"
            print "--job-name=\n"
            print "--queue-name=\n"
            print "--queue-priority=\n"
            print "--submit=\n"
            print "--out-of-hours=\n"
            print "e.g. to set the queue name to 'bob' write --queue-name=bob \n"
         else:
             print "Invalid argument "+ arg +"\nTry --help for more info.\n"

   else:
   
      # Name the queue
      clear_screen()
      print "What name do you want in the queue? (Up to 9 charicters)."
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
   
      # Run script check
      clear_screen()
      print "Do you want to run the script now?"
      input = str(raw_input( 'DEFAULT = ' + run_script_string + ' :\n'))
      if (len(input) != 0): run_script_string = input
      
      clear_screen()


   if debug:
      print "job name         = " + str(job_name)
      print "queue name       = " + str(queue_name)
      print "queue priority   = " + str(queue_priority )
      print "run script       = " + str(run_script_string)
      print "out of hours     = " + str(out_of_hours_string)

   return job_name, queue_priority, queue_name, run_script_string, out_of_hours_string;

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

def list_of_months_to_run( start_date, end_date):
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
      print month

   return months;


def create_the_input_files(months, debug=False):

# create folder input files 
   dir = os.path.dirname("input_files/")
   if not os.path.exists(dir):
      os.makedirs(dir)     


   first_month = True
# modify the input files to have the correct start months
   for month in months:
      end_time = month
      if first_month:
         first_month = False
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
         else: 
            newline = line
            output_file.write(newline)
      output_file.close()
      start_time = month
      input_geos.close()
   return;

def create_the_queue_files(months, queue_name, job_name, queue_priority, out_of_hours):

   
# create folder queue files 
   dir = os.path.dirname("queue_files/")
   if not os.path.exists(dir):
      os.makedirs(dir)     
      


   first_month = True
# modify the input files to have the correct start months
   for month in months:
      end_time = month
      if first_month:
         first_month = False
         start_time = month
         continue
      queue_file_location = os.path.join(dir , (start_time + ".pbs"))
      queue_file = open( queue_file_location, 'wb' )
# set PBS variables
      queue_file.write(
"""#PBS -j oe
#PBS -V
#PBS -q """ + str(queue_name) + """
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N """ + job_name + start_time + """
#PBS -r n
#
#PBS -o $PBS_O_WORKDIR/queue_output/
#
# Set priority.
#PBS -p """ + queue_priority )

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
ulimit -s 200000000 """)

      # If the run is only ment to start when it is not a work day:
      if out_of_hours:
         # get the hour from the system (date +"%H")
         queue_file.write("""

if  (( $(date +%u) < 5 ))  && (( 8 < $mytime )) && (($mytime < 18 )) ; then
   job_number=$(qsub -a 1800 queue_files/""" + str(start_time)+""".pbs)
   echo $job_number
   echo "tried running in work hours but we don't want to. Will try again at 1800. The time we attempted to run was:"
   date
   
fi
exit

""")

# Run geoschem
      queue_file.write("""

# Make sure using the spinup input file

#change to the directory that the command was issued from
cd $PBS_O_WORKDIR
rm -f input.geos
ln -s input_files/""" + str(start_time) +""".input.geos input.geos
/opt/sgi/mpt/mpt-2.09/bin/omplace ./geos > geos_"""+ str(start_time) + """.log
mv ctm.bpch """+str(start_time)+""".ctm.bpch
job_number=$(qsub queue_files/"""+str(end_time)+""".pbs)
echo $job_number
echo qdel $job_number > exit_geos.sh
chmod 775 exit_geos.sh
"""
   )
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




main( job_name, queue_priority, queue_name, run_script_string, out_of_hours_string, debug )
