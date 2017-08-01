# Need to run this as administrator
#Requires -RunAsAdministrator
param(
    $CertName = $null,
    $CertPassphrase = "test",
    $CN = $null,
    $CertFile = "server.pfx",
    [switch]$SelfSigned = $false
)

$ErrorActionPreference = "Stop"

$rootStore = new-object System.Security.Cryptography.X509Certificates.X509Store([System.Security.Cryptography.X509Certificates.StoreName]::My, "LocalMachine")
$rootStore.Open("MaxAllowed")

$rootCert = $($rootStore.Certificates | Where-Object { $_.FriendlyName -eq "RavenDB Server CA" }) | Select-Object -First 1
if ($rootCert -ne $null) {
    write-host "Found RavenDB Server CA. Using it as certificate issuer..."
}
else {
    write-host "RavenDB Server CA cert - producing self-signed certificate."
}

if ([string]::IsNullOrEmpty($CN)) {
    $CN = [System.Environment]::MachineName
}

if ([string]::IsNullOrEmpty($CertName)) {
    $certId = -Join ((65..90) | Get-Random -Count 5 | %{[char]$_})
    $CertName = "ravendb-server-$certId"; 
}

$signer = if ($SelfSigned) { $null } else { "cert:\LocalMachine\My\$($rootCert.Thumbprint)" }
Write-Host "Signing with $signer cert."

$subject = "C=IL,L=Hadera,OU=Ops,O=Hibernating Rhinos, CN=$CN"

Write-Host "Remember to make your CN match server url."
Write-Host "Friendly name is $CertName"
Write-Host "Subject is $subject"

$cert = New-SelfSignedCertificate `
     -Verbose `
     -NotAfter ([DateTime]::Today).AddYears(3) `
     -NotBefore ([DateTime]::Today).AddDays(-1) `
     -FriendlyName "$CertName" `
     -Subject $subject `
     -HashAlgorithm SHA256 `
     -CertStoreLocation Cert:\LocalMachine\My\ `
     -KeySpec Signature `
     -KeyUsageProperty All `
     -KeyUsage CertSign, CRLSign, DigitalSignature, KeyEncipherment `
     -TextExtension '2.5.29.37={text}1.3.6.1.5.5.7.3.2,1.3.6.1.5.5.7.3.1' `
     -Signer $signer

$certThumbprint = $cert.Thumbprint
$pfxPath = [io.path]::combine(".", $CertFile)
$certStorePath = "cert:\LocalMachine\My\$certThumbprint";

$passphrase = $CertPassphrase | ConvertTo-SecureString -AsPlainText -Force

Export-PfxCertificate -cert $certStorePath -FilePath $pfxPath -Password $passphrase -Force -Verbose

Write-Host "Done."