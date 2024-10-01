import os
import subprocess
import logging
import time

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from .utilities import loadConfiguration, checkFileStatus, get_relative_path


class LFSBlobClient(BlobServiceClient):
    def __init__(self, configurationFile='/etc/az_lfs_hsm_release.json', **kwargs) -> None:
        configuration = loadConfiguration(configurationFile)
        self.accountURL = configuration.get('accountURL')
        self.containerName = configuration.get('containerName')
        super().__init__(self.accountURL, credential=DefaultAzureCredential(exclude_workload_identity_credential=True, exclude_environment_credential=True), **kwargs)

    @staticmethod
    def rearchive(fullPath):
        logger = logging.getLogger()
        logger.info(f"{fullPath} starting rearchival process.")
        if not os.path.exists(fullPath):
            logger.error(f"{fullPath} doesn't exist.")
        elif "released" in str(subprocess.check_output(["lfs", "hsm_state", fullPath]).decode()):
            logger.error(f"{fullPath} is in released state. No action possible.")
        else:
            subprocess.check_output(["lfs", "hsm_set", "--dirty", fullPath])
            subprocess.check_output(["lfs", "hsm_archive", fullPath])
            while "ARCHIVE" in str(subprocess.check_output(["lfs", "hsm_action", fullPath]).decode()):
                logger.info(f"{fullPath} still rearchiving...")
                time.sleep(5)
            logger.info(f"{fullPath} rearchived.")

    def lfs_hsm_release(self, filePath):
        logger = logging.getLogger()

        absolutePath = os.path.abspath(filePath)
       
        client = self.get_blob_client(container=self.containerName, blob=get_relative_path(absolutePath))
        if checkFileStatus(absolutePath):
            if not client.exists():
                self.rearchive(absolutePath)

            try:
                subprocess.check_output(["lfs", "hsm_release", absolutePath])
            except subprocess.CalledProcessError as error:
                logger.error("Failed in setting hsm_state correctly. Please check the file status.")
                raise error