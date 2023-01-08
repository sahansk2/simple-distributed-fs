#!/usr/bin/env python3

from collections import Counter
from pathlib import Path
from typing import List
import logging
import random
import string
from socket import gethostname

logging.basicConfig(level=logging.DEBUG)

########### CONFIGURATION #############

def GET_LOGFILE_NAME(machine_id):
    return f"machine.{machine_id}.log"

def GET_RANDOM_LINE_PREFIX(machine_id):
    return "event_id=[{}/{}]".format(machine_id, "".join(random.choices(string.ascii_lowercase, k=15)))

## If ALL_MACHINES is strings, then SIGNATURES must use strings in its keys. 

ALL_MACHINES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

SIGNATURES = {
    "everywhere_token": {
        # "everywhere_token" will appear in all the log files 50% of the time
        "default": 0.5
    },
    "unique_token": {
        # "unique_token" will appear in only machines 1 and 2; 
        # machine 1 for 50% of the lines, and machine 2 for 30% of the lines.
        "default": 0,
        1: 0.5,
        #2: 0.3
    },
    "even_token": {
        # "even_machine_string" will appear in only machines, 2, 4, 6, 8, 10
        # for each, "even_machine_string" will appear in 
        "default": 0,
        2: 0.5,
        4: 0.5,
        6: 0.5,
        8: 0.5,
        10: 0.5
    },

    "heavy_token_1": {
        1: 0.5,
        "default": 0.05
    },
    "heavy_token_2": {
        1: 0.5,
        "default": 0.05
    },
    "heavy_token_3": {
        1: 0.5,
        "default": 0.05
    }
}

BATCH_SIZE = 100

_KB_UNIT = 1024
_MB_UNIT = _KB_UNIT * 1024

LOG_SIZE_IN_BYTES = 5 * _MB_UNIT

OUTPUT_DIRECTORY = "test_data/"
#######################################

out_dir_path = Path(OUTPUT_DIRECTORY)
if not out_dir_path.exists():
    out_dir_path.mkdir()

def should_yield_token(proportion, batch_size, current_line_no_zero_indexed):
    """
    Helper function for whether a token should appear on the current line number.
    For example, if the batch size is 200, the proportion is 0.4, and the current line 
    number is 90 (89 0-indexed), then this returns false. 

    Explanation:
    200 * 0.4 = 80; We are on line 89. Therefore, we shouldn't yield the token and this functino returns false.
    """
    return current_line_no_zero_indexed < proportion * batch_size


def get_token_proportion(machine_id, _expression):
    """
    Parse the signatures object for a specific machine_id, and represent the proportion of lines in the particular machine's log file
    that should have the expression.

    _expression is a tuple = (expression string, expression object).

    If the machine ID does not appear in the expression object, then we fallback to the "default" key.

    Example: if the input is (1, ("foobaz", { "default": 0, 1: 0.2 })), then 0.2 is returned.
    But, if the input is (2, ("foobaz", {"default": 0, 1: 0.2})), then 0 is returned.
    """
    expression_str, expression_obj = _expression
    set_value = None
    if machine_id not in expression_obj:
        if "default" not in expression_obj:
            logging.warn(f"Could not find proportion for {machine_id} and 'default' not set for expression {expression_str}. Assigning zero value.")
            set_value = 0
        else:
            set_value = expression_obj["default"]
    else:
        set_value = expression_obj[machine_id]
    
    if set_value < 0:
        logging.warn(f"Value for machine {machine_id}, expression {expression_str} is negative: {set_value}. Clamping to 0.")
        set_value = 0
    if set_value > 1:
        logging.warn(f"Value for machine {machine_id}, expression {expression_str} is over 1: {set_value}. Claping to 1.")
        set_value = 1
    
    logging.debug(f"For machine {machine_id}, expression {expression_str}, proportion is {set_value}")
    return set_value


def generate_line(sig_and_prop_pairs: List, lineno, batchsize, machine_id, true_signature_counter):
    output_line_parts = []
    for sig, prop in sig_and_prop_pairs:
        if lineno < batchsize * prop:
            # Add the token to the output line
            # TODO: refactor with an xeger implementation to support arbitrary regexes.
            output_line_parts.append(sig)
            true_signature_counter[sig] += 1

    return GET_RANDOM_LINE_PREFIX(machine_id) + " ".join(output_line_parts) + '\n'


# Need to make log files for each machine.
for machine_id in ALL_MACHINES:
    sig_and_prop_pairs = []
    for expr in SIGNATURES:
        sig_proportion_of_file = get_token_proportion(machine_id, (expr, SIGNATURES[expr]))
        # If the token is not of consideration, we ignore
        if sig_proportion_of_file == 0:
            continue
        else:
            sig_and_prop_pairs.append((expr, sig_proportion_of_file))
        
    sig_and_prop_pairs.sort(key=lambda pair: pair[1])
    logging.info("The signature and proportion pairs (sorted) is:")
    logging.info(sig_and_prop_pairs)

    # Now that we have a list of all the signatures, we will start to write the log file
    # We will write the log file in batches of 100. Why? Because (1) this makes it easy to
    # create an approximate proportion of the data we want (e.g. if the proportion is 0.57, then we just
    # need to write the token into the first 57 lines of the batch, and we repeat this for each batch)
    # and if we choose a very large batch size, then we might risk going over the desired filesize.
        
    machine_filename = GET_LOGFILE_NAME(machine_id)
    true_signature_counter = Counter()
    with open(Path(OUTPUT_DIRECTORY) / machine_filename, "wb") as logfile:
        logging.info(f"Now creating file {machine_filename} with size {LOG_SIZE_IN_BYTES}")
        # mystr.encode('utf-8')
        bytes_written = 0
        lines_written = 0
        while bytes_written < LOG_SIZE_IN_BYTES:
            # Write lineno lines at a time
            for lineno in range(BATCH_SIZE):
                next_line = generate_line(sig_and_prop_pairs, lineno, BATCH_SIZE, machine_id, true_signature_counter)
                nbytes = logfile.write(next_line.encode('utf-8'))
                bytes_written += nbytes
                lines_written += 1
    
        logfile.flush()
    
    signature_count_filename = f"{machine_filename}.counts.txt"
    with open(Path(OUTPUT_DIRECTORY) / signature_count_filename, 'w') as countfile:
        logging.info(f"Writing true signature counts of this file to {signature_count_filename}.")
        logging.info("Counts are:")
        logging.info(true_signature_counter)
        countfile.write(str(true_signature_counter))
    logging.info(f"Bytes written to file: {bytes_written}")
    logging.info(f"Lines written to file: {lines_written}")
