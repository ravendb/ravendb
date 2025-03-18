
 Official Docker images are available on our [Docker Hub](https://hub.docker.com/r/ravendb/ravendb/). 
 
 We provide images in two flavors: 
    
    - based on Ubuntu Linux (run on Linux containers) 

    - based on Windows Nanoserver image (run using Windows containers). 

### What's new
RavenDB `v6.0` docker image has been adjusted to run under **a non-root user**, providing far better security.
Containers are now using RavenDB *.deb package*. More information about how to deal with the breaking changes in the [**migration from 5.4 image**](#what-migration-process-from-54-image-looks-like) section.


### Image tags

The following tags are available:

#### Latest stable

- `latest` / `ubuntu-latest` - contains the latest version of RavenDB running on Ubuntu container

- `windows-1809-latest` - contains the latest version of RavenDB running running on Windows nanoserver (Windows version 1809)

- `windows-ltsc2022-latest` - contains the latest version of RavenDB running running on Windows nanoserver (Windows version 2022)

#### Latest LTS

- `latest-lts` / `ubuntu-latest-lts` - contains the latest LTS version of RavenDB running on Ubuntu container

- `windows-1809-latest-lts` - contains the latest LTS version of RavenDB running on Windows nanoserver (Windows version 1809)

- `windows-ltsc2022-latest-lts` - contains the latest LTS version of RavenDB running on Windows nanoserver (Windows version 2022)


*Note: windows-latest and windows-latest-lts tags were recently removed due to incompatibilities between different Windows versions and containers based on them.*

### Running

Simplest way to run and try RavenDB out is:

Linux image:

```
$ docker run -p 8080:8080 ravendb/ravendb:ubuntu-latest
```

Ubuntu ARM image:

```
$ docker run -p 8080:8080 ravendb/ravendb:ubuntu-arm32v7-latest
```

Windows image:

```
$ docker run -p 8080:8080 ravendb/ravendb:windows-latest
```

Optionally *nightly* images can be found at [ravendb/ravendb-nightly](https://hub.docker.com/r/ravendb/ravendb-nightly/)

You can run RavenDB docker container manually by invoking `docker run`, yet if you don't feel that docker-savvy we recommend using our scripts:

Run Ubuntu-based image: [run-linux.ps1](https://github.com/ravendb/ravendb/blob/v7.0/docker/run-linux.ps1)

Run Windows-based image: [run-nanoserver.ps1](https://github.com/ravendb/ravendb/blob/v7.0/docker/run-nanoserver.ps1)

Above mentioned Powershell scripts are simplifying usage of our images allowing you to pass various switches and options to configure RavenDB inside the container:

|Option|Default|Description|
| ---------------------------------- | :--------: | --------------------------------------------------------------------------------------------------------------------------------------- |
|`-DryRun`| | print `docker run` command and exit |
|`-LogsMode [log level]`| Operations | set logging level (Operations, Information) |
| `-ConfigPath [absolute file path]` |            | _absolute_ path to settings file used by RavenDB inside the container                                                                   |
| `-DataDir [absolute dir path]` || host directory mounted to the volume used for persistence of RavenDB data (if not provided a regular docker volume is going to be used) |
| `-BindPort [port]` | 8080 | the port number on which RavenDB Server is exposed on the container |
| `-BindTcpPort [port]` | 38888 | the port number on which RavenDB Server listens for TCP connections exposed on the container |
| `-NoSetup` | | disable setup wizard |
| `-RemoveOnExit` || removes container on server process exit |
| `-PublicServerUrl`                 |            | set the public url under which server is available to other nodes or admins (e.g. http://live-test.ravendb.net:80)                    |
| `-PublicTcpServerUrl` || set the url under which server is available to the outside world (e.g. tcp://live-test.ravendb.net:38888) |
| `-Unsecured` | | HERE BE DRAGONS - disable authentication for RavenDB server |

Once run RavenDB server should be exposed on port 8080 by default.

### Docker volumes

Each of the images above makes use of 2 volumes:

- settings volume - holding RavenDB configuration,

    Ubuntu container: `/etc/ravendb/`

    Windows container: `C:\RavenDB\Config`

- databases volume - used for persistence of RavenDB data,

    Ubuntu container: `/var/lib/ravendb/data`

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

### Dockerfiles

Dockerfiles that are used to build the images and their assets can be found at the following locations:

- [Ubuntu image](https://github.com/ravendb/ravendb/tree/v7.0/docker/ravendb-ubuntu)

- [Windows image](https://github.com/ravendb/ravendb/tree/v7.0/docker/ravendb-nanoserver)

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

##### What migration process from 5.4 image looks like?

Data is stored in a different directory, and needs to be migrated or linked upon update. The container is now using the .deb package, which is well documented [here](https://ravendb.net/docs/article-page/latest/csharp/start/installation/gnu-linux/deb). The link describes new filesystem locations and permissions. 

We highly recommend migrating your data from legacy data directory to the new one `/var/lib/ravendb/data/`.

However, running the server using `run-server.sh`, which is a default entry point for the container, causes to run `link-legacy-datadir.sh` script. It checks whether RavenDB data is stored under legacy data directory. If so, it tries to create symlink to the new data directory. If the permissions are insufficent for the 'ravendb' user (which is running the container) it is going to fail with an appropriate error message.

##### How to run as different UID?
To run with a different UID or GID, you need to build the Ubuntu image yourself with these build args (both are optional):

` --build-arg "RAVEN_USER_ID=999" --build-arg "RAVEN_GROUP_ID=999"`

The `ravendb` user will use the following UID/GID, it's being set in the .deb package post-installation process.

The default UID and GID are 999.

