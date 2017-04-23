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

- `-Detached` - runs the image in detached (background) mode - ([Docker's `-d`](https://docs.docker.com/engine/reference/run/#detached--d))

- `-DataVolumeName [volume name]` - default `ravendb` - the name of the volume used for persistence of RavenDB data

- `-Debug` - runs the shell on the container (Ubuntu: bash; Windows: Powershell) allowing you to debug the issue inside the container

- `-BindPort [port]` - default `8080` - the port number on which RavenDB Server is exposed on the container

- `-BindTcpPort [port]` - default `38888` - the port number on which RavenDB Server listens for TCP connections exposed on the container

NOTE: Script will attempt to create Docker volume, if does not exist for data persistence.

NOTE 2: Due to Windows containers limitations entire directory holding the settings file (passed via `-ConfigPath`) is going to be visible within the container.

Basic usage (saving data to `c:\docker\raven\databases` and using settings file mounted from host at `c:\docker\raven\settings.json`):
```
PS .\run-ubuntu1604.ps1 -DataDir "c:\docker\raven\databases" -ConfigPath "c:\docker\raven\settings.json"

Reading configuration from C:\path\to\settings.json.
Mounting c:\docker\raven\databases as RavenDB data dir.
       _____                       _____  ____
      |  __ \                     |  __ \|  _ \
      | |__) |__ ___   _____ _ __ | |  | | |_) |
      |  _  // _` \ \ / / _ \ '_ \| |  | |  _ <
      | | \ \ (_| |\ V /  __/ | | | |__| | |_) |
      |_|  \_\__,_| \_/ \___|_| |_|_____/|____/


      Safe by default, optimized for efficiency

 Build 40013, Version 4.0, SemVer 4.0.0-alpha-40013, Commit abcdefg
 PID 6, 64 bits
 Source Code (git repo): https://github.com/ravendb/ravendb
 Built with love by Hibernating Rhinos and awesome contributors!
+---------------------------------------------------------------+
Listening to: http://0.0.0.0:8080
Server started, listening to requests...
Running as Service
Tcp listening on 0.0.0.0:38888

```

Once run RavenDB server should be exposed on port 8080 by default on the container.

#### A bit on Docker network

Once you run the image, to find out the Docker container IP address use:

```
> docker ps

CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                 NAMES
01ca48f18cfe        935f3b389940        "/bin/sh -c /opt/r..."   About an hour ago   Up About an hour    8080/tcp, 38888/tcp   keen_poitras
```

Then use the `CONTAINER ID` to inspect container properties:
```
> docker inspect 01ca48f18cfe
...        
```

Among other fields there's *IPAddress* you can use to access Studio in your favorite web browser. Note, if you're running an Ubuntu container on Windows, please remember that your container is behind Hyper-V/VirtualBox NAT e.g. if you see `172.17.0.2` as an output of `docker inspect`, considering your docker NAT subnet address is `10.0.75.0` (See `Docker Settings -> Network -> Subnet Address`) and mask  `255.255.255.0`, then RavenDB Server is going to be accessible at `10.0.75.2:8080`.

#### On Docker volumes usage

Each of images above makes use of 2 volumes:

- settings volume - holding RavenDB configuration, meant to be mounted from one of host's files

    Ubuntu container: `/opt/raven-settings.json`

    Windows container: `C:\raven-config` directory

- databases volume - used for persistence of RavenDB data (you can either mount host directory to it using `-DataDir` or use persistent docker volume)

    Ubuntu container: `/databases`

    Windows container: `c:\databases`

### Building

These docker images are built using local artifacts. Dockerfile copies server archive of a given version from the project's `artifacts` directory and extracts it onto the Docker image.
