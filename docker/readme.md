## RavenDB Docker Support

The files here support building and running RavenDB 4.0 in a docker container on either Linux or Windows (nanoserver).

### Official images

 Official Docker images are available on our [Docker Hub](https://hub.docker.com/r/ravendb/ravendb/). We provide images in two flavors: ubuntu-based (to be run on Linux containers) and nanoserver-based (to be run using Windows containers). The following tags are available:

- `ubuntu-latest` - contains the latest version of RavenDB 4.0 running on Ubuntu 16.04 container

- `windows-nanoserver-latest` - contains the latest version of RavenDB 4.0 running running on Windows nanoserver

- every 4.0 release is going to have its own image set for both Ubuntu and Windows containers

- `latest` points to `ubuntu-latest`

### Running

Simplest way to run and try RavenDB out is:

Linux:
```
$ docker run -p 8080:8080 ravendb/ravendb
```

Windows:
```
$ docker run -p 8080:8080 ravendb/ravendb
```

You can run RavenDB manually invoking `docker run`, yet if you don't feel that docker-savvy we recommend using our scripts:

Run Ubuntu-based image: [run-ubuntu1604.ps1](run-ubuntu1604.ps1)

Run Windows-based image: [run-nanoserver.ps1](run-nanoserver.ps1)

Above mentioned Powershell scripts are simplifying usage of our images allowing you to pass various switches and options to configure RavenDB inside the container:

- `-ConfigPath [absolute file path]` - required - *absolute* path to settings file used by RavenDB inside the container

- `-DataDir [absolute dir path]` - host directory mounted to the volume used for persistence of RavenDB data (if not provided a regular docker volume is going to be used)

- `-DataVolumeName [volume name]` - default `ravendb` - the name of the volume used for persistence of RavenDB data

- `-BindPort [port]` - default `8080` - the port number on which RavenDB Server is exposed on the container

- `-BindTcpPort [port]` - default `38888` - the port number on which RavenDB Server listens for TCP connections exposed on the container

- `-NoSetup` - disable setup wizard

- `-AuthenticationDisabled` - HERE BE DRAGONS - disable authentication for RavenDB server

- `-RemoveOnExit` - removes container when server exits 

- `-PublicServerUrl` - set the url under which server is available to the outside world (e.g. http://4.live-test.ravendb.net:80)

- `-PublicTcpServerUrl` - set the url under which server is available to the outside world (e.g. tcp://4.live-test.ravendb.net:38888)

- `-LogsMode` - set logging level (Operations, Information)

Basic usage (saving data to `C:\docker\raven\databases` and using settings file mounted from host at `C:\docker\raven\settings.json`):
```
PS C:\work\ravendb-4\docker> .\run-ubuntu1604.ps1 -ConfigPath c:\work\docker\settings.json -DataDir C:\work\docker\databases
```

Once run RavenDB server should be exposed on port 8080 (default).

### Docker volumes

Each of images above makes use of 2 volumes:

- settings volume - holding RavenDB configuration,

    Ubuntu container: `/opt/RavenDB/config`
    Windows container: `C:\ravendb\config`

- databases volume - used for persistence of RavenDB data,

    Ubuntu container: `/opt/RavenDB/Server/RavenData`
    Windows container: `c:/ravendb/Server/RavenData`

### Configuration

To configure RavenDB one can use (in order of precedence):
    - environment variables, 
    - `settings.json` configuration file, 
    - CLI arguments

#### Environment variables

Environment variables prefixed with `RAVEN_` can be used to configure RavenDB server. E.g. one can use:
```
RAVEN_Setup_Mode='None'
```
to disable RavenDB Setup Wizard.

For docker containers one additional variable is available to modify CLI arguments line - `RAVEN_ARGS`. 

#### Enable Docker logs

To get logs available when running `docker logs` command, you need to turn that on for RavenDB server. Setting below environment variables like so is going to enable logging to console. Please note such behavior may have performance implications. Log level may be modified using `RAVEN_Logs_Mode` variable. 

```
RAVEN_ARGS='--log-to-console'
```

#### Custom config path

Use `--config-path PATH_TO_CONFIG` in order to use settings file from outside of server directory.

#### Dockerfiles

These images were built using the following Dockerfiles:

- [Windows Nanoserver image Dockerfile](https://github.com/ravendb/ravendb/blob/v4.0/docker/ravendb-nanoserver/Dockerfile)

- [Ubuntu 16.04 image Dockerfile](https://github.com/ravendb/ravendb/blob/v4.0/docker/ravendb-ubuntu1604/Dockerfile)
