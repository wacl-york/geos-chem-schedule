monthly_run
===========

Creates a script that splits up a single geoschem input file into multiple jobs that can be sent to the queue.

WARNING: Make a backup of your input file incase the script brakes it.( It makes a backup to input.geos.orig, so dont call the backup this)

INSTALL

To install download this repository with git clone either with MChem_tools or on its own.
Once downloaded, navigate to the monthly run folder containing monthly_run.py and run monthy_run.py --setup

This will provide a command that you can copy and paste, which will allow you to use the command "monthly_run" from any folder



The python script creates a bash submit script that you can run using:
BASH run_geos.sh

The script has a UI to chose job name, queue name, priority, if you want to start month jobs outside of work hours, and if you want to have the script submit the job to the queue.

The script can also take command line arguments. Type monthly_run.py --help for more info
monthly_run.py --job-name=bob --step=month --queue-name=run --queue-priotiry=100 --out-of-hours=yes --submit=yes

Explination:
This will call the job bob in the queue. It will split up the jobs into months. It will be submitted to the run queue with a priority of 100. The jobs will only start out of hours. If the job starts in working hours it will resubmit itself with a command to wait until 1800. The job will be submitted at the end of the script.


2016-11-14
Allow options for --step=week,day,month
Allow setup via monthly_run.py --setup
Code changes to make it more readable


2015-04-01
Updated the naming scheme of the log to month.geos.log
Changed from an error if the job name is over 9 characters to truncating the name to only 9 characters.

2015-02-18
Added options to submit arguments from the command line instead of the UI
Added an option to only start jobs out of work hours (0800-1800 Monday - Friday)
Changed the naming scheme of the logs.

2015-01-14
Option to send the run script to the queue streight away.
Option to name the job (up to 9 charicters).
Option to name the queue you wich to run on with error checking.
Option to chose priority of the jobs.
Now only sends one job to the queue at a time, reducing mess in qstat. Now calls then next month upon completion of the current month.


TO-DO

Allow splitting into 6 months or other multiples instead of just 1 month.



