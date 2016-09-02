# Test module for monthly run

"""
Py.test module for testing out monthly-run script
"""


from .monthly-run import *
import logging
logging.basicConfig(filename='test.log', level=logging.DEBUG)                   
logging.info('Started GC_funcs test')   

test_input_geos_file = 'test_files/input.geos'

def test_GET_INPUTS():
    logging.info("Begining test.")
    inputs = GET_INPUTS()

    input_list = vars(inputs)
    for item in input_list:
        assert isinstance(variable, str) == True,\
        "One of the inputs is not a string"


    assert (len(input_list) > 2),\
    "There dont appear to be enough inputs being created"
    logging.info("test complete")

def test_check_inputs():
    return

def test_backup_the_input_file():
    return

def test_get_argumens():
    return

def test_run_the_script():
    return

def test_get_start_and_end_dates():
    return

def test_list_of_months_to_run():
    return

def test_create_the_input_files():
    return


def create_the_queue_files():
    return

def create_the_run_script():
    return

logging.info("Finished monthly-run test.")



