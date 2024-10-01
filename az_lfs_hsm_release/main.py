import sys
import os
import logging
import argparse

from .lfs_blob_client import LFSBlobClient

def main():
    parser = argparse.ArgumentParser(prog='Azure LFS HSM Release', description='This utility helps managing released files from Azure Blob Lustre HSM backend.')

    args, extras = parser.parse_known_args()
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if not extras:
        logger.error('No filename specified.')
        sys.exit(1)

    fileToRelease = extras[-1]
    

    if os.path.isdir(fileToRelease):
        logger.error('HSM operates on files, not on folders. The input path refers to a folder.')
    elif os.path.exists(fileToRelease):
        LFSBlobClient(logger=logger).lfs_hsm_release(fileToRelease)
    else:
        logger.error('The file provided does not exist on the system')
    

if __name__ == '__main__':
    main()