function SignFile( $projectDir, $filePath, $dryRun ) {

    if ($dryRun) {
        Write-Host "[DRY RUN] Sign file $filePath.."
        return;
    }

    $signTool = "C:\Program Files (x86)\Windows Kits\8.1\bin\x64\signtool.exe"
    if (!(Test-Path $signTool))
    {
        $signTool = "C:\Program Files (x86)\Windows Kits\8.0\bin\x86\signtool.exe"

        if (!(Test-Path $signTool))
        {
            $signTool = "C:\Program Files\Microsoft SDKs\Windows\v7.1\Bin\signtool.exe"

            if (!(Test-Path $signTool))
            {
                throw "Could not find SignTool.exe under the specified path $signTool"
            }
        }
    }

    $installerCert = $env:CODESIGN_CERTPATH

    if (!(Test-Path $installerCert))
    {
        throw "Could not find pfx file under the path $installerCert to sign the installer"
    }

    $certPasswordPath = $env:CODESIGN_PASSPATH

    if (!(Test-Path $certPasswordPath))
    {
        throw "Could not find the path for the certificate password of the installer"
    }

    $certPassword = Get-Content $certPasswordPath
    if ($certPassword -eq $null)
    {
        throw "Certificate password must be provided"
    }

    Write-Host "Signing the following file: $filePath"

    $timeservers = @("http://tsa.starfieldtech.com", "http://timestamp.globalsign.com/scripts/timstamp.dll", "http://timestamp.comodoca.com/authenticode", "http://www.startssl.com/timestamp", "http://timestamp.verisign.com/scripts/timstamp.dll")
    foreach ($time in $timeservers) {
        try {
            &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "$time" /v /debug "$filePath"
            return
        }
        catch {
            continue
        }
    }

    throw "Could not reach any of the timeservers"
}
