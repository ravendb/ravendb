## Raven Docker Support

The files here support building and running ravendb 4.0 in a docker container on either Linux or Windows (nanoserver).

### Building

These docker images are built using the alpha downloads available on the website.  The Dockerfile downloads the zip files and extracts them onto the Docker image.

`build-{platform}.ps1` can be used to build and launch the docker images.

- `./build-{platform}.ps1 -Run -Wait` will build and run the docker image interactively: `docker run -it --rm ...`
- `./build-{platform}.ps1 -Run` will build and run the docker image detached: `docker run -d ...`

### Configuration

These images configure the DataDir to /databases or c:/databases, depending on platform.  They expose port 8080 and are configured with the `AllowEverybodyToAccessTheServerAsAdmin` flag set to true.  So the following execution:

#### Linux
`docker run -d -p 8080:8080 -v db:/databases ravendb`

#### Nanoserver
`docker run -d -p 8080:8080 -v db:c:/databases ravendb`

Would bind to port 8080 and mount the databases directory to the 'db' volume.
