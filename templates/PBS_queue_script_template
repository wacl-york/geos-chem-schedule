#!/bin/bash
#PBS -j oe
#PBS -V
#PBS -q {queue_name}
#     ncpus is number of hyperthreads - the number of physical core is half of that
#
#PBS -N {job_name}
#PBS -r n
#PBS -l walltime={wall_time}
#PBS -l mem={memory_need}
#PBS -l nodes=1:ppn={cpus_need}
#
#PBS -o queue_output/{start_time}.output
#PBS -e queue_output/{start_time}.error
#
# Set priority.
#PBS -p {queue_priority}


{email_string}


cd $PBS_O_WORKDIR

# Make sure the required dirs exists
mkdir -p queue_output
mkdir -p logs
mkdir -p PBS_queue_files

# Set environment variables
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


{out_of_hours_string}

# Change to the directory that the command was issued from
echo running in $PBS_O_WORKDIR > logs/log.log
echo starting on $(date) >> logs/log.log

# Create the exit file
echo qdel $job_number > exit_geos.sh
chmod 775 exit_geos.sh


rm -f input.geos
ln -s input_files/{start_time}.input.geos input.geos
/opt/hpe/hpc/mpt/mpt-2.16/bin/omplace ./geos > logs/{start_time}.geos.log

# Prepend the files with the date
mv ctm.bpch {start_time}.ctm.bpch
mv HEMCO.log logs/{start_time}.HEMCO.log

# Only submit the next month if GEOS-Chem completed correctly
last_line = "$(tail -n1 {start_time}.geos.log)"
complete_last_line = "**************   E N D   O F   G E O S -- C H E M   **************"

if [ "$last_line" = "$complete_last_line"]; then
   job_number=$(qsub PBS_queue_files/{end_time}.pbs)
   echo $job_number
fi
