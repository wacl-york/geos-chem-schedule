#!/usr/bin/python

# monthly-run.py

import os
import sys
import math
import shutil
import subprocess
# Inputs

# Automaticaly submit the generated run script
auto_submit = False

#Defaults
queue_name     = "GEOS"
queue_priority = "-1000"
queue_type     = "batch"
run_script     = "yes"

def main( queue_name, queue_priority, queue_type, run_script ):

   queue_name, queue_priority, queue_type, run_script = get_arguments( queue_name, queue_priority, queue_type, run_script)

   start_date, end_date = get_start_and_end_dates()

   months = list_of_months_to_run( start_date, end_date )
   
   create_the_input_files(months)
  
   create_the_queue_files(months, queue_type, queue_name, queue_priority)

   create_the_run_script(months)

   run_the_script(run_script)

   if auto_submit:
      print "complete"

   return;

def get_arguments(queue_name, queue_priority, queue_type, run_script):
   clear_screen()


   
   input = str(raw_input('What name do you want in the queue? (Up to 9 charicters) DEFAULT = ' + queue_name + ' :\n'))
   if (len(input) != 0): queue_name = input
   assert (len(queue_name) <= 9), "Queue name is too long," + str(len(queue_name)) + " charicters long"

   clear_screen()
   input = str(raw_input('What queue priority do you want? (Between -1024 and 1023). DEFAULT = ' + queue_priority + ' :\n'))
   if (len(input) != 0): queue_priority = input
   assert ((-1024 <= int(queue_priority)) and (int(queue_priority) <= 1023)), "Priority not within bounds of -1024 and 1023, recived " + str(queue_priority) 

   clear_screen()
   input = str(raw_input('What queue do you want to go in? DEFAULT = ' + queue_type + ' :\n'))
   if (len(input) != 0): queue_type = input
   assert ((queue_type=='core16') or (queue_type=='core32') or (queue_type=='core64') or (queue_type=='batch') or (queue_type=='run')), "Unrecognised queue type: " + str(queue_type)

   # Run script check
   clear_screen()
   input = str(raw_input('Do you want to run the script now? DEFAULT = ' + run_script + ' :\n'))
   if (len(input) != 0): run_script = input
   assert ((run_script=='yes') or (run_script=='y') or (run_script=='no') or (run_script=='n')), "Unrecognised queue type: " + str(queue_type)
   


   clear_screen()
   print "queue name        = " + queue_name
   print "queue priority    = " + queue_priority
   print "queue type        = " + queue_type
   print "run on completion = " + run_script

   return queue_name, queue_priority, queue_type, run_script;

def run_the_script(run_script):
   if ((run_script=='yes') or (run_script=="y")):
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

def create_the_queue_files(months, queue_type, queue_name, queue_priority):

   
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
      queue_file.write(
"""#PBS -j oe
#PBS -V
#PBS -q """ + str(queue_type) + """
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N """ + queue_name + start_time + """
#PBS -r n
#
#PBS -o $PBS_O_WORKDIR/queue_output/
#
# Set priority.
#PBS -p """ + queue_priority + """
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


def create_the_run_script(months):
   
   run_script = open('run_geos.sh','w') 
   run_script.write("#!/bin/bash \n")
   run_script.write("cp input.geos input.geos.orig \n")   

   first_month = True
   first_run   = True
   month_number = 0
# modify the input files to have the correct start months
   for month in months:
      month_number = month_number + 1
      end_time = month
      if first_month:
         first_month = False
         start_time = month
         continue
   
      dir = os.path.dirname("queue_files/")
      queue_file_location = os.path.join(dir , (start_time + ".pbs"))

      if first_run:
         first_run = False
         command = "MONTH"+str(start_time) + "=$(qsub " + queue_file_location + ") \n"
         command2 = "echo $MONTH"+str(start_time) + " \n"
         command3 = 'echo "#!/bin/bash" > exit_geos.sh \n'
         command4 = "echo qdel $MONTH"+str(start_time)+" >> exit_geos.sh \n"
         run_script.write(command)
         run_script.write(command2)
         run_script.write(command3)
         run_script.write(command4)
         
      else:
      
         command = "MONTH" + str(start_time) + "=$(qsub -W depend=afterok:$MONTH" + str(old_start_time) + " " + queue_file_location + ") \n"
         command2 = "echo $MONTH"+str(start_time) + " \n"
         command3 = "echo qdel $MONTH"+str(start_time)+" >> exit_geos.sh \n"
         run_script.write(command)
         run_script.write(command2)
         run_script.write(command3)
   
      old_start_time = start_time
      start_time = month


   run_script.close()
   return;   




main( queue_name, queue_priority, queue_type, run_script )
