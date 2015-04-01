#!/usr/bin/python



#Defaults

job_name             = "GEOS"          # Name of the job that appears in qstat
queue_priority       = "1000"         # Priority of the job
queue_name           = "batch"         # Name of the queue to submit too
run_script_string    = "yes"           # Do you want to auto submit the job with this script?
out_of_hours_string  = "no"            # Do you only want to run out of normal work hours? 
email_option         = "yes"            # Do you want to email after the final month has been completed?
email_address        = "bn506+PBS@york.ac.uk"
email_setting        = "e"             
debug=True



# If you have a script you would like to run upon completion, such as analyse some results, or turn results into a different format, then insert it in the run_completion_script function.

def run_completion_script(job_name):
   import subprocess
#   subprocess.call( 'create_netCDF_file' )
   return;
