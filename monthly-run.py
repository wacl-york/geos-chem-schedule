#!/usr/bin/python

# monthly-run.py

import os
import sys
import math
import shutil
# Inputs

# Name for the run in the queue, 10 charicters
queue_name = "GEOS"

# Priority, 0 is normal, 1023 is highest, -1024 is lowest
queue_priority = "0"
queue_type = "core16"


# Automaticaly submit the generated run script
auto_submit = False

def main():
   start_date, end_date = get_start_and_end_dates()

   months = list_of_months_to_run( start_date, end_date )
   
   create_the_input_files(months)
  
   create_the_queue_files(months, queue_type, queue_name, queue_priority)

   create_the_run_script(months)

   if auto_submit:
      print "complete"

   return;

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
#PBS -q """ + queue_type + """
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N """ + queue_name + """
#PBS -r n
#
#
# Set priority.
#PBS -p """ + str(queue_priority) + """
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
cp input_files/""" + str(start_time) +""".input.geos input.geos
/opt/sgi/mpt/mpt-2.09/bin/omplace ./geos > geos_"""+ str(start_time) + """.log
mv ctm.bpch """+str(start_time)+""".ctm.bpch
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
         run_script.write(command)
         run_script.write(command2)
      else:
      
         command = "MONTH" + str(start_time) + "=$(qsub -W depend=afterok:$MONTH" + str(old_start_time) + " " + queue_file_location + ") \n"
         command2 = "echo $MONTH"+str(start_time) + " \n"
         run_script.write(command)
         run_script.write(command2)
   
      old_start_time = start_time
      start_time = month


   run_script.close()
   return;   




main()
