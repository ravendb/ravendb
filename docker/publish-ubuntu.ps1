param(
    [string]$Repo = "ravendb/ravendb",
    [string]$ArtifactsDir = "..\artifacts",
    [string]$Arch = "x64",
    [switch]$DryRun = $False,
    [switch]$RemoveImages = $False,
    [switch]$UseVersionTagsOnly
)

$ErrorActionPreference = "Stop"

. ".\common.ps1"

if ($env:DRY_RUN) {
    $DryRun = $True
}

function PushImagesToDockerHub([string[]]$imageTags) {
    write-host "Pushing images to Docker Hub."
    foreach ($tag in $imageTags) {
        write-host "Push $tag"
        docker push "$tag"
        CheckLastExitCode
    }
}

function PushImagesDryRun([string[]]$imageTags) {
    write-host "DRY RUN: Pushing images."
    foreach ($tag in $imageTags) {
        write-host "DRY RUN: docker push $tag"
    }
}

function PushImages([string[]]$imageTags) {
    if ($DryRun -eq $False) {
        PushImagesToDockerHub $imageTags
    }
    else {
        PushImagesDryRun $imageTags
    }
}

function RemoveImages($imageTags) {
    write-host "Removing images."
    foreach ($tag in $imageTags) {
        write-host "Remove $tag"
        docker rmi "$tag"
        CheckLastExitCode
    }
}

$version = GetVersionFromArtifactName
[string[]]$tags = GetUbuntuImageTags $Repo $version $Arch $UseVersionTagsOnly
PushImages $tags

if ($RemoveImages) {
    RemoveImages $tags
}
