from pathlib import Path
import sys
import unittest
import subprocess
import argparse
import warnings
import logging 
from socket import gethostname 
from datetime import datetime

logging.basicConfig(level=logging.FATAL)

BASE_PORT = 6968
NUM_LOGGERS = 10

LOGS_DIR = Path('../unit_test_out/').absolute()
LOCAL_OUT_PATH = (LOGS_DIR / "local_output.log").absolute()
SCRIPTS = Path('../scripts/').absolute()
LOGGER_CWD = (SCRIPTS / "test_data").absolute()
TEST_DATA_DIR = LOGGER_CWD
QUERIER_OUT_PATH = (LOGS_DIR / "querier_out.log").absolute()
GOFILE_PATH = Path('../main').absolute()

QUERIER_PATH = GOFILE_PATH / 'querier'
QUERIER_LOGLEVEL = 'fatal'
LOGGER_LOGLEVEL = 'fatal'
LOGGER_PATH = GOFILE_PATH / 'logger'

COMMON_FILE = 'machine.2.log'

def GET_VM_FILTER(i):
    return f"{gethostname()}:{str(BASE_PORT + i)}" 

def build_binaries():
    global QUERIER_PATH
    global LOGGER_PATH

    now = datetime.now()
    suffix = now.strftime(r"%H.%M.%S-%d.%m.%Y")
    querier_name = f"querier_{suffix}"
    logger_name = f"logger_{suffix}"
    subprocess.call(['go', 'build', '-o', querier_name, 'querier.go'], cwd=GOFILE_PATH)
    subprocess.call(['go', 'build', '-o', logger_name, 'logger.go'], cwd=GOFILE_PATH)

    QUERIER_PATH = GOFILE_PATH / querier_name
    LOGGER_PATH = GOFILE_PATH / logger_name

def validate_paths():
    paths = [LOGS_DIR, SCRIPTS]
    nonexistent = []
    for p in paths:
        if not p.exists():
            nonexistent.append(p)
    if nonexistent:
        logging.error("Paths don't exist!")
        print(",".join(nonexistent), file=sys.stderr)

def trace_annot(func):
    def newfn(*args, **kwargs):
        logging.debug(f"Now entering {func.__name__}")
        func(*args, **kwargs)
        logging.debug(f"Now exiting {func.__name__}")
    return func

@trace_annot
def generate_logs():
    logging.debug("Generating logs...")
    p = subprocess.run(["python3", "generate_test_data.py"])
    if p.returncode:
        logging.error("Failed to generate test data. Quite sad innit")
        exit(1)


@trace_annot
def run_loggers():
    """
        Runs all loggers on ports increasing from BASE_PORT.

        @output: All loggers write into LOGS_DIR / logger_i_out.log
        @return: none
    """


    logging.debug("Verifying whether test_data exists...")
    if not LOGGER_CWD.exists():
        generate_logs()

    logger_cmd = [LOGGER_PATH, "-loglevel", LOGGER_LOGLEVEL, "-port"]
    logging.debug(f"Running {NUM_LOGGERS} loggers...")
    for i in range(1, NUM_LOGGERS + 1):
        logger_cmd.append(str(BASE_PORT + i))
        with open(LOGS_DIR / f"logger_{i}_out.log", "w") as logfile:
            subprocess.Popen(logger_cmd, stdout=logfile, stderr=logfile, cwd=LOGGER_CWD)
        logger_cmd.pop()


@trace_annot
def kill_loggers():
    """
        Kills all loggers.

        @return: none 
    """
    
    p = subprocess.run(["pkill", "-f", "logger*"])
    logging.debug(f"Killing {NUM_LOGGERS} loggers...")
    if p.returncode != 0:
        logging.error(f"Failed to kill all loggers. Returned with {p.returncode}")


# SHOULD BE GOOD?
@trace_annot
def run_querier(grep_cmd, demo_mode=False):
    """
        Runs querier with specified grep command

        @param grep_cmd: Full grep command to run with querier 
        @param demo_mode: Specifies whether to use -D flag -- grep on machine.i.log
        @output: Writes into LOGS_DIR / querier_out.log
        @return: none
    """


    logging.debug(f"Running querier...")
    if demo_mode:
        # Implicit filename, machine.i.log
        querier_cmd = [QUERIER_PATH, "-U", str(NUM_LOGGERS), "-loglevel", QUERIER_LOGLEVEL, "-D"]
        querier_cmd.extend(grep_cmd.split(" "))
    else:
        querier_cmd = [QUERIER_PATH, "-U", str(NUM_LOGGERS), "-loglevel", QUERIER_LOGLEVEL]
        querier_cmd.extend(grep_cmd.split(" "))
        querier_cmd.append(COMMON_FILE)
    

    with open(QUERIER_OUT_PATH, "w") as logfile, open(Path('/dev/null'), 'w') as devnull:
        p = subprocess.run(querier_cmd, stdout=logfile, stderr=devnull)
        if p.returncode != 0:
            logging.error(f"Failed to run querier. Returned with {p.returncode}")
        logging.debug(f"Querier output written to logs/querier_out.log...")


@trace_annot
def parse_querier_log(machine_id):
    """
        Uses querier log as input, and feeds into grep | sed to search for one VM and 
        filter metadata.

        @param machine_id: id of VM to search for 
        @output: Writes into LOGS_DIR / vm_i_output.log
        @return none
    """


    logging.debug(f"Parsing querier log for VM: {machine_id}...")
    with open(LOGS_DIR / f"vm_{machine_id}_output.log", "w") as logfile:
        subprocess.call(f"grep {GET_VM_FILTER(machine_id)} {str(QUERIER_OUT_PATH)} | sed -E 's/^.+://'",
                        shell=True, stdout=logfile, stderr=logfile)


@trace_annot
def locally_grep_machine_log(machine_id, grep_cmd, demo_mode):
    """
        Runs grep command on PC on machine.i.log - use to validate output of
        querier.

        @param machine_id: id of VM
        @param grep_cmd: full grep command, except for output file. This function runs
                         grep on machine.i.log. Grep command also has token
        @output Write sinto LOGS_DIR / local_output.log
        @return none 
    """

    logging.debug(f"Running local grep on machine.{machine_id}.log...")
    with open(LOCAL_OUT_PATH, "w") as logfile:
        if demo_mode:
            path = str(TEST_DATA_DIR / f"machine.{machine_id}.log")
        else:
            path = str(TEST_DATA_DIR / COMMON_FILE)
        subprocess.call(f"{grep_cmd} {path} | sed -E 's/^.+://'",
                        shell=True, stdout=logfile, cwd=LOGGER_CWD)


@trace_annot
def run_diff(file1, file2):
    """
        Runs diff utility -- used for comparing output of querier to output of running
        grep on PC
        
        @param file1: path of first file 
        @param file2: path of second file 
        @return: True if the two files are the same, false when diff output is non empty
    """


    with open(LOGS_DIR / "diff.out", "w") as diff_out:
        subprocess.run(["diff", file1, file2], stdout=diff_out)

    with open(LOGS_DIR / "diff.out", "r") as diff_out:
        out = diff_out.readlines()

    if len(out) > 0:
        print("-----------------------------------")
        print(f"Files {file1} and {file2} differ!")
        print("-----------------------------------")
        return False
    return True



class TestDistributedGrep(unittest.TestCase):
    def run_test_helper(self, grep_cmd, demo_mode=True):
        rc = True
        for i in range(1, NUM_LOGGERS + 1):
            
            run_querier(grep_cmd, demo_mode)
            parse_querier_log(i)
            locally_grep_machine_log(i, grep_cmd, demo_mode)
    
            rc = run_diff(LOCAL_OUT_PATH, LOGS_DIR / f"vm_{i}_output.log") and rc 
        return rc

    # Tests to make sure that querier results from localhost-hosted loggers
    #  are consistent with results created from locally running grep
    # To see where these tokens came from, see scripts/generate_test_data.py.
    
    # Everywhere token appears on all machines
    def test_everywhere_token(self):
        grep_cmd = "grep -nic everywhere_token"
        rc = self.run_test_helper(grep_cmd)
        self.assertTrue(rc)

    # Unique token only appears on machine 1.
    def test_unique_token(self):
        grep_cmd = "grep -nicE unique_token"
        rc = self.run_test_helper(grep_cmd)
        self.assertTrue(rc)

    # Even token appears in half of the VM logs.
    def test_even_token(self):
        grep_cmd = "grep -nicE even_token"
        rc = self.run_test_helper(grep_cmd)
        self.assertTrue(rc)

    # Validate that grepping an identical file results in identical output on all VMs
    def test_identical_file(self):
        grep_cmd = "grep -nic everywhere_token"
        rc = self.run_test_helper(grep_cmd, demo_mode=False) # Filename has to eventually be passed to the querier
        self.assertTrue(rc)

    # Make sure that our querier can run with some basic regex
    def test_regex_string(self):
        grep_cmd = "grep -nicE heavy_token_."
        rc = self.run_test_helper(grep_cmd)
        self.assertTrue(rc)

    @classmethod
    def setUpClass(self):
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("ignore")
            kill_loggers()
            build_binaries()
            if not LOGS_DIR.exists():
                LOGS_DIR.mkdir()
            validate_paths()
            run_loggers()

    @classmethod
    def tearDownClass(self):
        kill_loggers()
        QUERIER_PATH.unlink()
        LOGGER_PATH.unlink()

parser = argparse.ArgumentParser()

unittest.main()
