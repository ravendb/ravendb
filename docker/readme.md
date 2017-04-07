## RavenDB Docker Support

The files here support building and running RavenDB 4.0 in a docker container on either Linux or Windows (nanoserver).

### Building

These docker images are built using local artifacts. Dockerfile copies server archive and extracts it onto the Docker image.

### Configuration

These images configure the `DataDir` to /databases or c:/databases, depending on platform. They expose port 8080 and are configured with the `AllowEverybodyToAccessTheServerAsAdmin` flag set to `false` by default.

To run the image run script called `run_ubuntu1604.ps1` or `run_windows.ps1` depending on your platform. The following switches are supported to simplify usage:

    -Detached - runs the image in detached mode (Docker's `-d`)

    -AllowEverybodyToAccessTheServerAsAdmin - sets RavenDB's setting `/Raven/AllowEverybodyToAccessTheServerAsAdmin` (default: false)

    -BindPort 8080 - sets the port to which to bind container's RavenDB Server (default: 8080)

    -DbVolumeName - sets the Docker volume name used to persist data (default: ravendb)

NOTE: `run_X.ps1` script will attempt to create Docker volume, if does not exist.
