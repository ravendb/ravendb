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
                $signTool = "C:\Program Files (x86)\Microsoft SDKs\ClickOnce\SignTool\signtool.exe"

                if (!(Test-Path $signTool)) 
                {
                    throw "Could not find SignTool.exe under the specified path $signTool"
                }
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
    $timeservers = @(
        "http://timestamp.digicert.com",
        "http://timestamp.globalsign.com/tsa/r6advanced1",
        "http://rfc3161timestamp.globalsign.com/advanced",
        "http://timestamp.sectigo.com",
        "http://timestamp.apple.com/ts01",
        "http://tsa.mesign.com",
        "http://time.certum.pl",
        "https://freetsa.org",
        "http://tsa.startssl.com/rfc3161",
        "http://dse200.ncipher.com/TSS/HttpTspServer",
        "http://zeitstempel.dfn.de",
        "https://ca.signfiles.com/tsa/get.aspx",
        "http://services.globaltrustfinder.com/adss/tsa",
        "https://tsp.iaik.tugraz.at/tsp/TspRequest",
        "http://timestamp.entrust.net/TSS/RFC3161sha2TS"
    )
    foreach ($time in $timeservers) {
        try {
            Write-Host "Command: $signTool sign /f `"$installerCert`" /p `"PASSWORD`" /d `"RavenDB`" /du `"https://ravendb.net`" /t `"$time`" /v /debug `"$filePath`""
            exec { & $signTool sign /f "$installerCert" /p "$certPassword" /fd SHA256 /d "RavenDB" /du "https://ravendb.net" /tr "$time" /td SHA256 /v /debug "$filePath" }
            CheckLastExitCode
            return
        }
        catch {
            continue
        }
    }

    throw "Error signing $filePath - see SignTool.exe error above."
}
