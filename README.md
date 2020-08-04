# geos-chem-schedule

Reads a GEOS-Chem (http://geos-chem.org/) run directory's input file (`input.geos`) and uses this to split up a job multiple jobs. These jobs can be as part of this process or manually submitted at a later point.

This script intends to make it easier to split up GEOS-Chem jobs into smaller parts that can better fit into queues available on an high performance computing (HPC) facility. Currently this script is compatible with PBS and SLURM.




## Install

To install download this repository with git clone. Recommended location would be `$HOME/src/geos-chem-schedule`.
Once downloaded, you can just add a symbolic link for the `geos-chem-schedule.py` to a run directory and run by calling directly in that directory (e.g. `Python geos-chem-schedule.py`). Alternatively you can install to be run through bash and this is done by navigating to the geos-chem-schedule folder containing geos-chem-schedule.py and running the following steps:

1. Download the script and run the setup with the following commands.

```bash
mkdir -p $HOME/src
cd $HOME/src
git clone https://github.com/wacl-york/geos-chem-schedule.git
python geos-chem-schedule/geos-chem-schedule.py --setup
```

OR

If you have **AC_tools** downloaded (https://github.com/tsherwen/AC_tools), Go to your **AC_tools** dir and do the following commands.

```bash
git submodule update --recursive --remote
python Scripts/geos-chem-schedule/geos-chem-schedule.py --setup
```

2.
Copy and paste the command provided into the terminal, which will allow you to use the command "geos-chem-schedule.py" from any folder

3.
Edit your settings.json file for options like default memory requirements, default run queue, default job name, and add your email address.


## Use

### Via Python

Create a symbolic link in the GEOS-Chem run directory and then call this and follow the steps on screen

```bash
ln -s <route to geos-chem-schedule>/geos-chem-schedule/geos-chem-schedule.py .
Python geos-chem-schedule.py
```

### Via bash

Go to your GEOS-Chem run directory and confirm your input.geos file is correct.
type the following command if you have followed up the setup:

```bash
geos-chem-schedule
```

Follow the UI instructions on screen.

The final option allows you to run the script immediately, or if you want to run the command later, it creates a file run_geos.sh. This file can be executed by typing:

```bash
bash run_geos.sh
```

The script has a UI to chose job name, queue name, priority, if you want to start the jobs outside of work hours, and if you want to have the script submit the job to the queue.

The script can also take command line arguments. Type geos-chem-schedule.py --help for more info. This can be useful if you have lots of simulations you want to send off via a script.

For example:
```bash
geos-chem-schedule.py --job-name=bob --step=month --queue-name=run --queue-priority=100 --out-of-hours=yes --submit=yes
```

This will call the job bob in the queue. It will split up the jobs into months. It will be submitted to the run queue with a priority of 100. The jobs will only start out of hours. If the job starts in working hours it will resubmit itself with a command to wait until 1800. The job will be submitted at the end of the script.


## WARNINGS:

If using bpch output for GEOS-Chem instead of the default NetCDF (v11+), then note this script forces bpch output to be produced (setting=3) for the end of simulation date and replaces all other days with a 0. If you want every day to run with a 3 then use --step=daily.


## History

 - 2020-07-31

Updated to allow use of script with SLURM schedular
General re-writing of functions for clarity and addition of documentation
Various fixes applied (inc. enable email functionality for SLURM jobs)
Repository name updated to geos-chem-schedule to reflect functionality

 - 2016-11-14

Allow options for --step=week,day,month
Allow setup via geos-chem-schedule.py --setup
Code changes to make it more readable

 - 2015-04-01

Updated the naming scheme of the log to YYYYMMDD.geos.log
Changed from an error if the job name is over 9 characters to truncating the name to only 9 characters.

 - 2015-02-18

Added options to submit arguments from the command line instead of the UI
Added an option to only start jobs out of work hours (0800-1800 Monday - Friday)
Changed the naming scheme of the logs.

 - 2015-01-14

Option to send the run script to the queue straight away.
Option to name the job (up to 9 characters).
Option to name the queue you which to run on with error checking.
Option to chose priority of the jobs.
Now only sends one job to the queue at a time, reducing mess in qstat. Now calls then next month upon completion of the current month.

