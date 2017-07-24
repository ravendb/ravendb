## RavenDB Docker Support

The files here support building and running RavenDB 4.0 in a docker container on either Linux or Windows (nanoserver).

### Official images

 Official Docker images are available on our [Docker Hub](https://hub.docker.com/r/ravendb/ravendb/). We provide images in two flavors: ubuntu-based (to be run on Linux containers) and nanoserver-based (to be run using Windows containers). The following tags are available:

- `ubuntu-latest` - contains the latest version of RavenDB 4.0 running on Ubuntu 16.04 container

- `windows-nanoserver-latest` - contains the latest version of RavenDB 4.0 running running on Windows nanoserver

- every 4.0 release is going to have its own image set for both Ubuntu and Windows containers

- `latest` points to `ubuntu-latest`

### Running

You can run RavenDB manually invoking `docker run`, yet if you don't feel that docker-savvy and would like to make full use of RavenDB docker images we recommend using our scripts.

#### Scripts

Run Ubuntu-based image: [run-ubuntu1604.ps1](run-ubuntu1604.ps1)

Run Windows-based image: [run-nanoserver.ps1](run-nanoserver.ps1)

Above mentioned Powershell scripts are simplifying usage of our images allowing you to pass various switches and options to configure RavenDB inside the container:

- `-ConfigPath [absolute file path]` - required - *absolute* path to settings file used by RavenDB inside the container

- `-DataDir [absolute dir path]` - host directory mounted to the volume used for persistence of RavenDB data (if not provided a regular docker volume is going to be used)

- `-DataVolumeName [volume name]` - default `ravendb` - the name of the volume used for persistence of RavenDB data

- `-BindPort [port]` - default `8080` - the port number on which RavenDB Server is exposed on the container

- `-BindTcpPort [port]` - default `38888` - the port number on which RavenDB Server listens for TCP connections exposed on the container

- `-AuthenticationDisabled` - HERE BE DRAGONS - disable authentication for RavenDB server

- `-RemoveOnExit` - removes container when the main process exits

- `-PublicServerUrl` - set the url under which server is available to the outside world (e.g. http://4.live-test.ravendb.net:80)

- `-PublicTcpServerUrl` - set the url under which server is available to the outside world (e.g. tcp://4.live-test.ravendb.net:38888)

NOTE: Script will attempt to create Docker volume, if does not exist for data persistence.

NOTE 2: Due to Windows containers limitations entire directory holding the settings file (passed via `-ConfigPath`) is going to be visible within the container.

Basic usage (saving data to `c:\docker\raven\databases` and using settings file mounted from host at `c:\docker\raven\settings.json`):
```
PS C:\work\ravendb-4\docker> .\run-ubuntu1604.ps1 -ConfigPath c:\work\docker\settings.json -DataDir C:\work\docker\databases
Mounting C:\work\docker\databases as RavenDB data dir.
Reading configuration from c:\work\docker\settings.json
Starting container: docker run -d -v C:\work\docker\databases:/databases -v c:\work\docker\settings.json:/opt/raven-settings.json -p 8080:8080 -p 38888:38888 ravendb/ravendb:ubuntu-latest
**********************************************

RavenDB docker container running.
Container ID is f01bdfbe111ffa3fd5b9217459bad776fe1e99877108315dc2a42de0cb644768

To stop it use:     docker stop f01bdfbe111ffa3fd5b9217459bad776fe1e99877108315dc2a42de0cb644768
To run shell use:   docker exec -it f01bdfbe111ffa3fd5b9217459bad776fe1e99877108315dc2a42de0cb644768 /bin/bash

Access RavenDB Studio on http://10.0.75.2:8080
Listening for TCP connections on: 10.0.75.2:38888

Container IP address in Docker network: 172.17.0.2
Docker bridge iface address: 10.0.75.1

**********************************************
PS C:\work\ravendb-4\docker>
```

Once run RavenDB server should be exposed on port 8080 (default).

#### On Docker volumes usage

Each of images above makes use of 2 volumes:

- settings volume - holding RavenDB configuration,

    Ubuntu container: `/opt/raven-settings.json`
    Windows container: `C:\raven-config` directory

- databases volume - used for persistence of RavenDB data,

    Ubuntu container: `/databases`
    Windows container: `c:\databases`
