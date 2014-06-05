monthly-run
===========

Creates a script that splits up a single geoschem input file and sends each month to the queue

It can read the current input file, and then split it up into the files, then sub them into the queue.

WARNING: Make a backup of your input file incase it brakes it.( It makes a backup to input.geos.orig, so dont call the backup this)

The python script creates a bach script that you need to CHMOD and run after the python script completes


TO-DO

Add a flag to auto run the bash script

Add a auto remove from the queue.



