param(
    $CertPath = ".\server.pfx",
    $ClientCertName = "client-test",
    $ClientCertPassword = $null,
    $ServerUrl = $null,
    $SecurityClearance = "ClusterAdmin"
)

$ErrorActionPreference = "Stop"

if ($ServerUrl -eq $null) {
    $ServerUrl = "https://" + [System.Environment]::MachineName + ":8080"
}

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ServerUrl = $ServerUrl.TrimEnd("/")

$ValidSecClearance = @( 'ClusterAdmin', 'Operator' )

if (($ValidSecClearance | Where-Object { $_ -eq $SecurityClearance } | Select-Object -First 1) -eq $null) {
    write-host "Invalid security clearance option. Provide one of the following: $ValidSecClearance"
    exit 1;
}

$payload = @{
    Name              = $ClientCertName;
    SecurityClearance = $SecurityClearance; # ClusterAdmin, Operator
    Password          = $ClientCertPassword;
} | ConvertTo-Json

$serverCert = Get-PfxCertificate -FilePath $CertPath;
$url = "$ServerUrl/admin/certificates"

write-host "Sending request to server"
write-host
write-host "POST $url"
write-host 
write-host $payload

try {
    Invoke-WebRequest `
        -Method POST `
        -Certificate $serverCert `
        -Body $payload `
        -ContentType "application/json" `
        -OutFile "$ClientCertName.pfx" `
        -ErrorVariable RestError `
        -ErrorAction SilentlyContinue `
        $url

    write-host "Generate client certificate $ClientCertName.pfx with $SecurityClearance security clearance"
}
catch {
    if ($_.Exception.Response -ne $null) {
        Write-Host $_.Exception.Message

        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        Write-Host $reader.ReadToEnd()
    }

    Write-Error $_.Exception
}

