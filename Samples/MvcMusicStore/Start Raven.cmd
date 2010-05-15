REM this scripts assumes the directory structure of the released zip
REM  starts the server in debug mode in the current directory, meaning that
REM  it will look for a default.raven file and load it if it exists
copy ..\..\Server\RavenDB.* .\
RavenDB.exe