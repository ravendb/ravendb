## RavenDB Docker Support

The files here support building and running RavenDB 4.2 in a docker container on either Linux or Windows (nanoserver).

### Official images

 Official Docker images are available on our [Docker Hub](https://hub.docker.com/r/ravendb/ravendb/). We provide images in two flavors: ubuntu-based (to be run on Linux containers) and nanoserver-based (to be run using Windows containers). The following tags are available:

- `4.2-ubuntu-latest` - contains the latest version of RavenDB 4.2 running on Ubuntu 18.04 container

- `4.2-windows-nanoserver-latest` - contains the latest version of RavenDB 4.2 running running on Windows nanoserver

- every 4.2 release is going to have its own image set for both Ubuntu and Windows containers

### Running

Simplest way to run and try RavenDB out is:

Linux image:
```
$ docker run -p 8080:8080 ravendb/ravendb:4.2-ubuntu-latest
```

Windows image:
```
$ docker run -p 8080:8080 ravendb/ravendb:4.2-windows-nanoserver-latest
```

Optionally nightly images can be used from [ravendb/ravendb-nightly](https://hub.docker.com/r/ravendb/ravendb-nightly/)

You can run RavenDB docker container manually by invoking `docker run`, yet if you don't feel that docker-savvy we recommend using our scripts:

Run Ubuntu-based image: [run-linux.ps1](https://github.com/ravendb/ravendb/blob/v4.2/docker/run-linux.ps1)

Run Windows-based image: [run-nanoserver.ps1](https://github.com/ravendb/ravendb/blob/v4.2/docker/run-nanoserver.ps1)

Above mentioned Powershell scripts are simplifying usage of our images allowing you to pass various switches and options to configure RavenDB inside the container:

|Option|Default|Description|
|------|:-----:|-----------|
|`-DryRun`| | print `docker run` command and exit |
|`-LogsMode [log level]`| Operations | set logging level (Operations, Information) |
|`-ConfigPath [absolute file path]` | | *absolute* path to settings file used by RavenDB inside the container |
| `-DataDir [absolute dir path]` || host directory mounted to the volume used for persistence of RavenDB data (if not provided a regular docker volume is going to be used) |
| `-BindPort [port]` | 8080 | the port number on which RavenDB Server is exposed on the container |
| `-BindTcpPort [port]` | 38888 | the port number on which RavenDB Server listens for TCP connections exposed on the container |
| `-NoSetup` | | disable setup wizard |
| `-RemoveOnExit` || removes container on server process exit |
| `-PublicServerUrl` || set the public url under which server is available to other nodes or admins (e.g. http://4.live-test.ravendb.net:80)
| `-PublicTcpServerUrl` || set the url under which server is available to the outside world (e.g. tcp://4.live-test.ravendb.net:38888) |
| `-Unsecured` | | HERE BE DRAGONS - disable authentication for RavenDB server |

Once run RavenDB server should be exposed on port 8080 by default.

### Docker volumes

Each of images above makes use of 2 volumes:

- settings volume - holding RavenDB configuration,

    Ubuntu container: `/opt/RavenDB/config`

    Windows container: `C:\RavenDB\Config`

- databases volume - used for persistence of RavenDB data,

    Ubuntu container: `/opt/RavenDB/Server/RavenData`

    Windows container: `C:/RavenDB/Server/RavenData`

### Configuration

To configure RavenDB one can use (in order of precedence):

    - environment variables, 

    - `settings.json` configuration file, 

    - CLI arguments

#### Environment variables

Environment variables prefixed with `RAVEN_` can be used to configure RavenDB server. E.g. one can use:
```bash
RAVEN_Setup_Mode='None'
```
to disable RavenDB Setup Wizard.

#### FAQ

##### I'm using compose / doing automated installation. How do I disable setup wizard?
    
Set `Setup.Mode` configuration option to `None` like so:
```bash
RAVEN_Setup_Mode='None'
```

##### I want to try it out on my local / development machine. How do I run unsecured server?

Set env variables like so:
```bash
RAVEN_Setup_Mode='None'
RAVEN_Security_UnsecuredAccessAllowed='PrivateNetwork'
```

##### How can I pass command line arguments?

By modifying `RAVEN_ARGS` environment variable. It's passed as an CLI arguments line.

##### Can I see RavenDB logs by running `docker logs`?

To get logs available when running `docker logs` command, you need to turn that on for RavenDB server. Setting below environment variables like so is going to enable logging to console. Please note such behavior may have performance implications. Log level may be modified using `RAVEN_Logs_Mode` variable. 

```bash
RAVEN_ARGS='--log-to-console'
```

##### How to set a custom config file?

Mount it as a docker volume and use `--config-path PATH_TO_CONFIG` command line argument in order to use settings file from outside of server directory.

#### Dockerfiles

These images were built using the following Dockerfiles:

- [Windows Nanoserver image Dockerfile](https://github.com/ravendb/ravendb/blob/v4.2/docker/ravendb-nanoserver/Dockerfile)

- [Ubuntu 18.04 image Dockerfile](https://github.com/ravendb/ravendb/blob/v4.2/docker/ravendb-linux/Dockerfile)
