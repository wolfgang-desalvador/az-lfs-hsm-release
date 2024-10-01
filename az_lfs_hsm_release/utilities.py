import json
import subprocess
import logging
import os

def loadConfiguration(file):
    with open(file, 'r') as fid:
        configuration = json.load(fid)
    return configuration

def checkFileStatus(filePath):
    try:
        fileStatus = str(subprocess.check_output(["lfs", "hsm_state", filePath]))
    except subprocess.CalledProcessError as error:
        logging.error('LFS command failed with error {}. Are you sure you are running the utility on a Lustre mount?'.format(str(error)))

    if 'released' in fileStatus:
        logging.error(f"The file {filePath} is not restored to Lustre and cannot be released to backend. Please restore the file to Lustre before.")
        return False
    elif 'archived' not in fileStatus:
        logging.error(f"The file {filePath} does not appear to be in the backend according to lfs hsm_state.")
        return False
    else:
        return True
    
def get_relative_path(path):
    mountPath = os.path.abspath(path)
    while not os.path.ismount(mountPath):
        mountPath = os.path.dirname(mountPath)

    relativePath = os.path.abspath(path).replace(mountPath, "")    

    if relativePath[0] == '/':
        relativePath = relativePath[1:]
    
    return relativePath