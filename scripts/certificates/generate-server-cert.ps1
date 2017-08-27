# Need to run this as administrator
#Requires -RunAsAdministrator
param(
    $CertName = $null,
    $CertPassphrase = "test",
    $CN = $null,
    $CertFile = "server.pfx",
    $SignerName = "RavenDB Server CA"
)

$ErrorActionPreference = "Stop"

$rootStore = new-object System.Security.Cryptography.X509Certificates.X509Store(
    [System.Security.Cryptography.X509Certificates.StoreName]::AuthRoot,
    "localmachine"
)

$rootStore.Open("MaxAllowed")

if ([string]::IsNullOrEmpty($CN)) {
    $CN = [System.Environment]::MachineName
}

if ([string]::IsNullOrEmpty($DNS)) {
    $DNS = [System.Environment]::MachineName
}

if ([string]::IsNullOrEmpty($CertName)) {
    $certId = -Join ((65..90) | Get-Random -Count 5 | %{[char]$_})
    $CertName = "ravendb-server-$certId"; 
}

$existingCert = $($rootStore.Certificates | Where-Object { $_.FriendlyName -eq $SignerName }) | Select-Object -First 1

if ($existingCert -eq $null) {
   
    $existingCert = New-SelfSignedCertificate `
        -CertStoreLocation "cert:\LocalMachine\My" `
        -HashAlgorithm sha256 `
        -NotAfter ([DateTime]::Today).AddYears(3) `
        -NotBefore ([DateTime]::Today).AddDays(-1) `
        -FriendlyName $SignerName `
        -Subject "CN=ca.hrhinos.local,O=Hibernating Rhinos,OU=Ops" `
        -Type Custom `
        -KeySpec Signature `
        -KeyUsageProperty All `
        -KeyUsage CertSign, CRLSign, DigitalSignature, KeyEncipherment `
        -KeyExportPolicy Exportable 

    $rootStore.Add($existingCert);
    write-host "Added self signed certificate $SignerName to Trusted Root Certification Authorities collection: $($existingCert.Thumbprint)"
}



$subject = "O=operations.ravendb.local, CN=$CN"

Write-Host "Remember to make your Common Name (CN) match the server url."
Write-Host
Write-Host "Friendly name is $CertName"
Write-Host "Subject is $subject"

$cert = New-SelfSignedCertificate `
        -Verbose `
        -NotAfter ([DateTime]::Today).AddYears(3) `
        -NotBefore ([DateTime]::Today).AddDays(-1) `
        -FriendlyName "$CertName" `
        -Subject $subject `
        -Dnsname $DNS `
        -HashAlgorithm SHA256 `
        -CertStoreLocation Cert:\LocalMachine\My\ `
        -KeySpec Signature `
        -KeyUsageProperty All `
        -KeyUsage CertSign, CRLSign, DigitalSignature, KeyEncipherment `
        -TextExtension '2.5.29.37={text}1.3.6.1.5.5.7.3.2,1.3.6.1.5.5.7.3.1' `
        -Signer $existingCert

$certThumbprint = $cert.Thumbprint
$pfxPath = [io.path]::combine(".", $CertFile)
$certStorePath = "cert:\LocalMachine\My\$certThumbprint";

$passphrase = $CertPassphrase | ConvertTo-SecureString -AsPlainText -Force

Export-PfxCertificate -cert $certStorePath -FilePath $pfxPath -Password $passphrase -Force -Verbose

Write-Host "Done."