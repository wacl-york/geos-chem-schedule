#
"""
Script to set up monthly-run for a new user.
Creates the defaults file for default settings.
Creates sym-links in your /bin folder so that you can run
the programme from the command line.
"""

# Modules needed
import os.path

def main():

    print "Setting up monthly-run"

    create_default_settings_file()

    create_symlinks()

    print "monthly-run setup complete"
    print "you can now run the script with 'monthly-run' from any folder from \
            when you next log on."
    print "Or if you can't wait, run the command below:"
    print "source ~/.bashrc"

    return

def clear_screen():                                                             
   os.system('cls' if os.name == 'nt' else 'clear')                             
   return    


def create_default_settings_file():

    """
    Creates a 'monthly-run.cfg' file if one does not already
    exist
    """


    if os.path.isfile('monthly-run.cfg'):
        print "'monthly-run.cfg' already exists. Not creating a new one."
    else:
        import csv
        import csv

        cfg = {}
        

        print "Setting up a new config file for monthly-run"
        str(raw_input( \
            "Press 'Enter' to continue or 'CTRL-C' to quit.\n" \
            ))
        
        clear_screen()

        cfg_file = open('monthly-run.cfg', 'w')


        file_help = [
"Config file for monthly-run.py",
"Edit the other json fields to change the default.",
"This text just contains lots of information about the options.",
"",
"email:",
"This is the email address the queue will send emails too if",
"the settings are enabled.",
"",
"email_option:",
"Set yes to receve emails. Set no to not.",
"",
"email_setting:",
"Set e to email on error. Other settings can be seen in the PBS man page.",
"",
"job_name:",
"Set the default job name. Recomended trailing _ as dates are appended",
"to the job name on run.",
"",
"queue_priority:",
"Should be 0.",
"",
"queue_name:",
"Choose the default queue you want to run on. Probably the 'run' queue.",
"",
"out_of_hours_string:",
"Want to be nice? ",
"Set this to yes if you want to only run nights and weekends.",
"Reccomended to yes if you don't need results eargently, ",
"no if you need results fast.",
"",
"wall_time:",
"How long does your run definatly complete in.",
"Given in HH:MM:SS",
"Setting this low may reduce queue times.",
"Setting this too low will mean you are kicked from your run before it completes!",
"",
"memory_need:",
"How much memory do you need?",
"can use gb. e.g. '10gb'.",
"Setting this low may reduce queue time.",
"Setting this too low will slow your program down or break it!",
"",
"run_script_string:",
"This program makes a set of queue scripts for you that are set up to",
"call the next script on completeion. If you want this program to run",
"the script also then set this to 'yes'. If however for some reason you",
"would rather run the script yourself mannualy, this can be set to 'no'.",
"",
        ]


        # Get username for email
        cfg['email'] = str(getpass.getuser()) + '+PBS@york.ac.uk'
        
        # Dissable email by default
        cfg['email_option'] = 'no'

        cfg['email_setting'] = "e"

        cfg['job_name'] = "GEOS_"
    
        cfg['queue_priority'] = "0"

        cfg["queue_name"] = "run"

        cfg["out_of_hours_string"] = "no"

        cfg["wall_time"] = "24:00:00"

        cfg["memory_need"] = "10gb"

        cfg["run_script_string"] = "yes"


        cfg_file.write(file_help)
        for item in cfg:

            json.dump( cfg, cfg_file, sort_keys=True, indent=4 )

    return
        

def create_symlinks():
    """
    Creates symbolik links to this file in the ~/bin folder.
    Makes sure the file is exacutable.
    """

    import os
    import inspect


    home_dir = os.path.expanduser('~')
    bin_dir = os.path.join(home_dir , 'bin')
    monthly_run_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    monthly_run_script_path = os.path.join(monthly_run_dir, 'monthly-run.py')

    print monthly_run_script_path
    print bin_dir
    
    # Create the bin dir
    if not os.path.exists( bin_dir ):
        os.mkdirs( bin_dir )
    
    # Make exacutable
    if not os.path.isfile( monthly_run_script_path ):
        print "cant find file"
    os.chmod(monthly_run_script_path, 0755)
    

    # Create the sym link
#    os.symlink(monthly_run_scripy_path, os.path.join(bin_dir, 'monthly_run')

    return

if __name__ == '__main__':
    main()
