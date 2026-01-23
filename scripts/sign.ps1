function SignFile( $projectDir, $filePath, $dryRun ) {

    if ($dryRun) {
        Write-Host "[DRY RUN] Sign file $filePath.."
        return;
    }

    $requiredEnvVars = @("AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID")
    foreach ($var in $requiredEnvVars) {
        if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($var))) {
            throw "Missing required environment variable for Azure Auth: $var. Ensure this is set in TeamCity parameters."
        }
    }

    $signTool = $null

    if (![string]::IsNullOrWhiteSpace($env:BUILD_TOOLS_PATH)) {
        $customPattern = Join-Path $env:BUILD_TOOLS_PATH "Microsoft.Windows.SDK.BuildTools\bin\*\x64\signtool.exe"
        Write-Host "Searching BUILD_TOOLS_PATH: $customPattern"

        $signTool = Get-ChildItem -Path $customPattern -ErrorAction SilentlyContinue | 
                    Where-Object { $_.Exists } |
                    Sort-Object @{Expression={[System.Diagnostics.FileVersionInfo]::GetVersionInfo($_.FullName).FileVersion}; Descending=$true} | 
                    Select-Object -First 1 -ExpandProperty FullName
    }

    if ([string]::IsNullOrWhiteSpace($signTool)) {
        $standardPattern = "C:\Program Files (x86)\Windows Kits\10\bin\*\x64\signtool.exe"
        Write-Host "Searching Standard Path: $standardPattern"

        $signTool = Get-ChildItem -Path $standardPattern -ErrorAction SilentlyContinue | 
                    Where-Object { $_.Exists } |
                    Sort-Object @{Expression={[System.Diagnostics.FileVersionInfo]::GetVersionInfo($_.FullName).FileVersion}; Descending=$true} | 
                    Select-Object -First 1 -ExpandProperty FullName
    }

    if ([string]::IsNullOrWhiteSpace($signTool)) {
        $signTool = "C:\Program Files (x86)\Microsoft SDKs\ClickOnce\SignTool\signtool.exe"

        if (!(Test-Path $signTool)) {
            throw "Could not find SignTool.exe under the specified path $signTool"
        }
    }

    $azureLibPath = $env:CODESIGN_AZURELIB_PATH
    if (!(Test-Path $azureLibPath)) {
        throw "Could not find Azure Signing lib path at: $azureLibPath"
    }

    if ([string]::IsNullOrWhiteSpace($env:CODESIGN_METADATA_CONTENT)) {
        throw "Missing environment variable: CODESIGN_METADATA_CONTENT (JSON content required)."
    }

    $tempMetadataPath = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $tempMetadataPath -Value $env:CODESIGN_METADATA_CONTENT -Encoding UTF8
    Write-Host "Created temporary metadata file at: $tempMetadataPath"

    try {
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
                Write-Host "Command: $signTool /fd SHA256 /tr `"$time`" /td SHA256 /d `"RavenDB`" /du `"https://ravendb.net`" /dlib `"$azureLibPath`" /dmdf `"$metadataPath`" /v /debug `"$filePath`""
                exec { & $signTool sign /fd SHA256 /tr "$time" /td SHA256 /d "RavenDB" /du "https://ravendb.net" /dlib "$azureLibPath" /dmdf "$tempMetadataPath" /v /debug "$filePath" }
                
                CheckLastExitCode
                Write-Host "Successfully signed: $filePath"
                return
            }
            catch {
                Write-Warning "Failed to sign with $time. Error: $($_.Exception.Message)"
                continue
            }
        }

        throw "Error signing $filePath - All timestamp servers failed or SignTool encountered a critical error."
    }
    finally {
        if (Test-Path $tempMetadataPath) {
            Remove-Item -Path $tempMetadataPath -Force -ErrorAction SilentlyContinue
            Write-Host "Cleaned up temporary metadata file."
        }
    }
}
