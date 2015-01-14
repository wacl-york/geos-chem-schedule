monthly-run
===========

Creates a script that splits up a single geoschem input file and sends each month to the queue

It can read the current input file, and then split it up into the files, then sub them into the queue.

WARNING: Make a backup of your input file incase it brakes it.( It makes a backup to input.geos.orig, so dont call the backup this)

The python script creates a bach script that you need to CHMOD and run after the python script completes

Or you can do :
BASH run_geos.sh

The BASH script generates a exit script on run, so you can remove all the jobs the run script created from the queue:
BASH exit_geos.sh

2015-01-14
Option to send the run script to the queue streight away.
Option to name the job (up to 9 charicters).
Option to name the queue you wich to run on with error checking.
Option to chose priority of the jobs.
Now only sends one job to the queue at a time, reducing mess in qstat. Now calls then next month upon completion of the current month.


TO-DO

Allow splitting into 6 months or other multiples instead of just 1 month.



