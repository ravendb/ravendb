
mkdir c:/ravendb
mkdir c:/logs

$acl = Get-Acl c:\logs
$permission = @("ANONYMOUS LOGON","FullControl","Allow" )
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule $permission
$acl.SetAccessRule($rule)
$acl | Set-Acl c:\logs

Expand-Archive c:/ravendb.zip -DestinationPath c:/ravendb
Remove-Item c:\ravendb.zip