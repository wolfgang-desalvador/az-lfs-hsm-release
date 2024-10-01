# Azure LFS HSM Release
This repository contains an implementation to allow hsm-release on Azure Managed Lustre.

# Prerequisites

This utilities rely on a Managed Identity with propoer permissions on the Azure Blob Storage acting as Lustre HSM backend.

More specifically it requires:
* A managed identity with Storage Blob Data Contributor role on the relevant container for Lustre HSM
* A configuration file in `/etc/az_lfs_hsm_release.json` containing account URL and container name, with the following format:

```json
{
    "accountURL": "https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/",
    "containerName": "<LUSTRE_HSM_CONTAINER_NAME>"
}
```

## Installation

Ideally, this package is meant to run on Python 3. It is suggested to create a virtual environment, for example:

```bash
python3 -m venv $HOME/az_lfs
source $HOME/az_lfs/bin/activate
```

To install the package, download the wheel from the Releases and just perform:

```bash
source $HOME/az_lfs/bin/activate
pip install <WHEEL_FILE>
```

In case you get error in `cryptography` installation, just perform an upgrade of `pip`:

```bash
source $HOME/az_lfs/bin/activate
pip install --upgrade pip
pip install <WHEEL_FILE>
```

## Usage

In order to release a file from the Lustre backend, the file should be released on the Lustre file system. If that is not the case, the command will print an error and it won't perform any operation

```bash
az_lfs_hsm_release <file_path>
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.