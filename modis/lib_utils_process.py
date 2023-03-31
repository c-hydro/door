"""
Library Features:

Name:          lib_utils_process
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20230331'
Version:       '1.0.0'
"""
# -------------------------------------------------------------------------------------
# Libraries
import os
import subprocess
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to make bash file executable
def make_bash_exec(path):
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to run a bash file
def run_bash_exec(script_bash):
    exec_handle = subprocess.Popen(script_bash)
    #  shell=True, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    exec_handle.communicate()
# -------------------------------------------------------------------------------------
