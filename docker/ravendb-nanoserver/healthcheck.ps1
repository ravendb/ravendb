
$serviceStatus = (Get-Service -Name "RavenDB").Status
if (($serviceStatus -eq "Running") -or ($serviceStatus -eq "StartPending")) {
    exit 0;
} else {
    exit 1;
}