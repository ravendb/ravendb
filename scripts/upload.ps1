# TODO @gregolsky update regexes for stable
$CATEGORIES = @(
    @('RavenDB-[0-9]\.[0-9]\.[0-9]-[a-zA-Z]+-[0-9]+-windows-x64', "RavenDB for Windows x64"),
    @('RavenDB-[0-9]\.[0-9]\.[0-9]-[a-zA-Z]+-[0-9]+-ubuntu\.14\.04-x64', "RavenDB for Ubuntu 14.04 x64"),
    @('RavenDB-[0-9]\.[0-9]\.[0-9]-[a-zA-Z]+-[0-9]+-ubuntu\.16\.04-x64', "RavenDB for Ubuntu 16.04 x64")
)

function Get-UploadCategory ( $filename ) {
    $result = [io.path]::GetFilenameWithoutExtension($filename)
    foreach ($category in $CATEGORIES) {
        $categoryPattern = $category[0]
        if ($filename -match $categoryPattern) {
            $result = $category[1]
            break
        }
    }

    $result
}

function UploadArtifact ( $uploader, $buildNumber, $buildType, $filename, $log ) {
    $uploadCategory = Get-UploadCategory $filename
    $versionString = "$buildNumber-$((Get-Culture).textinfo.toTitleCase($buildType))"

    write-host "Executing: $uploader ""$uploadCategory"" ""$versionString"" $filename ""$log"""
    $uploadTryCount = 0
    while ($uploadTryCount -lt 5) {
        $uploadTryCount += 1

        & $uploader "$uploadCategory" ""$versionString" $file "$log"

        if ($lastExitCode -ne 0) {
            write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount"
        }
        else {
            break
        }
    }

    if ($lastExitCode -ne 0) {
        write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount. Build will fail."
        throw "Error: Failed to publish build"
    }
}

function Upload ( $uploader, $buildNumber, $buildType, $artifacts ) {
    Write-Host "Starting upload"

    if ($(Test-Path $uploader) -eq $False) {
        throw "$uploader not found."
    }

    $log = git log -n 1 --oneline
    $log = $log.Replace('"','''') # avoid problems because of " escaping the output

    foreach ($filename in $artifacts)
    {
        UploadArtifact $uploader $buildNumber $buildType $filename $log
    }
}
