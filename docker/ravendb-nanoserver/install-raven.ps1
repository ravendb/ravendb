New-Item -Type Directory -Force -Path c:/ravendb
Expand-Archive c:/ravendb.zip -DestinationPath c:/ravendb -Force
Remove-Item c:\ravendb.zip
