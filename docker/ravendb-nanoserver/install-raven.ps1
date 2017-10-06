New-Item -Type Directory -Force -Path c:/ravendb | Out-Null
Expand-Archive c:/ravendb.zip -DestinationPath c:/ravendb -Force
Remove-Item c:\ravendb.zip
