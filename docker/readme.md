## Raven Docker Support

The files here support building and running ravendb 4.0 in a docker container on either linux or windows.

### Process

The current process is:

1. Download latest official Windows or Linux builds
2. Extract the files from those downloads into the correct 'ravendb-linux' or 'ravendb-windows' folders (Client, Server, etc... should be directly under ravendb-{platform})
3. Run build-linux.ps1 or build-windows.ps1
4. Launch docker containers with these images with:

**TODO: Complete**