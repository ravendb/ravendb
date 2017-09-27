param(
    $CertPath = ".\server.pfx",
    $ClientCertName = "client-test",
    $ClientCertPassword = $null,
    $ServerUrl = $null,
    $SecurityClearance = "ClusterAdmin"
)

if($ServerUrl -eq $null) {
   $ServerUrl = "https://" +  [System.Environment]::MachineName  + ":8080"
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
    Name = $ClientCertName;
    SecurityClearance = $SecurityClearance; # ClusterAdmin, Operator
    Password = $ClientCertPassword;
} | ConvertTo-Json

$serverCert = Get-PfxCertificate -FilePath $CertPath;
$url = "$ServerUrl/admin/certificates"

write-host "Sending request to server"
write-host
write-host "POST $url"
write-host 
write-host $payload

[Net.ServicePointManager]::ServerCertificateValidationCallback = [System.Net.Security.RemoteCertificateValidationCallback] {
        param($sender, $certificate, $chain, $sslPolicyErrors)
    
        # validate whether server cert is used for obtaining client cert
        $result = $certificate.Thumbprint -eq $serverCert.Thumbprint
        if ($result -eq $False) {
            throw "Certificate used for obtaining client certificate must be same as server certificate."
        }
    
        return $result
    }

Invoke-WebRequest `
    -Method POST `
    -Certificate $serverCert `
    -Body $payload `
    -ContentType "application/json" `
    -OutFile "$ClientCertName.pfx" `
    -ErrorVariable RestError `
    -ErrorAction SilentlyContinue `
    $url

if ($RestError)
{
    $HttpStatusCode = $RestError.ErrorRecord.Exception.Response.StatusCode.value__
    $HttpStatusDescription = $RestError.ErrorRecord.Exception.Response.StatusDescription
    
    Throw "Http Status Code: $($HttpStatusCode) `nHttp Status Description: $($HttpStatusDescription)"
}

write-host "Generate client certificate $ClientCertName.pfx with $SecurityClearance security clearance"
