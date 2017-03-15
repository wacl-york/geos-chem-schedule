monthly_run
===========

Creates a script that splits up a single GEOS-Chem input file into multiple jobs that can be sent to the queue.
This allows lots of smaller jobs to be ran so that they can fit into different queues, and are also fairer on the system.


INSTALL
==================================================

To install download this repository with git clone. Recommended location would be $HOME/src/monthly_run
Once downloaded, navigate to the monthly_run folder containing monthly_run.py and run:

1) Download the script and run the setup with the following commands.

```bash
mkdir -p $HOME/src
cd $HOME/src
git clone https://github.com/wacl-york/monthly_run.git
python monthly_run/monthly_run.py --setup
```

OR

If you have AC_tools downloaded, Go to your AC_tools dir and do the following commands.

```bash
git submodule update --recursive --remote
python Scripts/monthly_run/monthly_run.py --setup
```

2)
Copy and paste the command provided into the terminal, which will allow you to use the command "monthly_run" from any folder

3)
Edit your settings.json file for options like default memory requirements, default run queue, default job name, and add your email address.


WARNINGS:
==================================================

Make a backup of your input file in case the script brakes it.( It makes a backup to input.geos.orig, so don't call the backup this)

The script forces saving the CSPEC to on.

The script forces 3 for the end of simulation date and replaces all other days with a 0. If you want every day to run with a 3 then use --step=daily.


USE:
==================================================

Go to your GEOS-Chem run directory and confirm your input.geos file is correct.
type the following command if you have followed up the setup:

```bash
monthly_run
```

Follow the UI instructions on screen.

The final option allows you to run the script immediately, or if you want to run the command later, it creates a file run_geos.sh. This file can be executed by typing:

```bash
bash run_geos.sh
```

The script has a UI to chose job name, queue name, priority, if you want to start month jobs outside of work hours, and if you want to have the script submit the job to the queue.

The script can also take command line arguments. Type monthly_run.py --help for more info.
This can be useful if you have lots of simulations you want to send off via a script.

example:
```bash
monthly_run.py --job-name=bob --step=month --queue-name=run --queue-priority=100 --out-of-hours=yes --submit=yes
```
Explanation:
This will call the job bob in the queue. It will split up the jobs into months. It will be submitted to the run queue with a priority of 100. The jobs will only start out of hours. If the job starts in working hours it will resubmit itself with a command to wait until 1800. The job will be submitted at the end of the script.



HISTORY:
==================================================

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
Option to send the run script to the queue straight away.
Option to name the job (up to 9 characters).
Option to name the queue you which to run on with error checking.
Option to chose priority of the jobs.
Now only sends one job to the queue at a time, reducing mess in qstat. Now calls then next month upon completion of the current month.





