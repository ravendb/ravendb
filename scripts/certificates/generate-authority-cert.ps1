# Need to run this as administrator
#Requires -RunAsAdministrator

$ErrorActionPreference = 'Stop'

$CERT_FRIENDLY_NAME = "RavenDB Server CA";

$rootStore = new-object System.Security.Cryptography.X509Certificates.X509Store(
    [System.Security.Cryptography.X509Certificates.StoreName]::Root,
    "localmachine"
)

$rootStore.Open("MaxAllowed")
try {
    $existingCert = $($rootStore.Certificates | Where-Object { $_.FriendlyName -eq $CERT_FRIENDLY_NAME }) | Select-Object -First 1

    if ($existingCert -ne $null) {
        Write-Host "Certificate already exists: $($existingCert.FriendlyName) $($existingCert.Thumbprint)";
        exit 0;
    }

    $cert = New-SelfSignedCertificate `
        -CertStoreLocation "cert:\LocalMachine\My" `
        -HashAlgorithm sha256 `
        -NotAfter ([DateTime]::Today).AddYears(3) `
        -NotBefore ([DateTime]::Today).AddDays(-1) `
        -FriendlyName $CERT_FRIENDLY_NAME `
        -Subject "CN=ca.hrhinos.local,O=Hibernating Rhinos,OU=Ops" `
        -Type Custom `
        -KeySpec Signature `
        -KeyUsageProperty All `
        -KeyUsage CertSign, CRLSign, DigitalSignature, KeyEncipherment `
        -KeyExportPolicy Exportable 

    $rootStore.Add($cert);
    write-host "Added certificate to Trusted Root Certification Authorities collection: $($cert.Thumbprint)"
}
finally {
    $rootStore.Close();
}
