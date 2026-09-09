WillisAPI Client is the official Python client for interfacing with WillisAPI.

WillisAPI allows for sharing data between third-party systems and the Willis platform, developed and maintained by Brooklyn Health. 

To learn more about Willis or Brooklyn Health, please visit [brooklyn.health](https://brooklyn.health).

## Installation

Before you begin, we recommend creating a virtual environment for the client and using a Python version later than 3.10.

```shell
pip install virtualenv
virtualenv willisapi_client --python=3.10
source willisapi_client/bin/activate
```

To install WillisAPI client, run:

```shell
pip install willisapi_client
```

After installation is complete, enter a python environment. Then, import the library:

```py
import willisapi_client as willisapi
```

## Access

To use any of the client’s functions, the user will need a personal access token (PAT).

This key will be provided to authorized users by Brooklyn Health staff. To get your key, please reach out to your technical contact at Brooklyn Health.

## Documentation

For complete documentation on use, please visit the [WillisAPI Client Wiki](https://github.com/bklynhlth/willisapi_client/wiki).
