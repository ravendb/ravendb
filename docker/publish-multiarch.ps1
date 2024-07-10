param(
    $Repo = "ravendb/ravendb",
    [switch]$DryRun = $False)

$ErrorActionPreference = "Stop"

. ".\common.ps1"

if ($env:DRY_RUN) {
    $DryRun = $True
}

function CreateDockerManifests {
    param (
        [string]$repo,
        [string[]]$manifestTags,
        [string[]]$tags
    )

    Write-Host "Creating docker manifests combining: $tags"

    foreach ($manifestTag in $manifestTags) {
        Write-Host "Creating docker manifest $manifestTag."
        
        $command = "docker manifest create $manifestTag"
        foreach ($tag in $tags) {
            $command += " --amend $tag"
        }

        Invoke-Expression $command
    }
}

function PushDockerManifests {
    param (
        [string[]]$manifests
    )

    Write-Host "Pushing manifests to docker."

    foreach ($manifest in $manifests) {
        if ($DryRun) {
            Write-Host "DRY RUN: docker manifest push $manifest"
        }
        else {
            Write-Host "Pushing manifest $manifest"
            docker manifest push $manifest
        }
    }
    
}

$tags = GetImageTagsForManifest $Repo
$manifestTags = GetManifestTags $Repo

CreateDockerManifests $Repo $manifestTags $tags
PushDockerManifests $manifestTags
