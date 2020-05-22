param(
    $Repo = "ravendb/ravendb", 
    $ArtifactsDir = "..\artifacts",
    $Arch = "x64",
    [switch]$DryRun = $False,
    [switch]$RemoveImages = $False)

$ErrorActionPreference = "Stop"

. ".\common.ps1"

function PushImagesToDockerHub($imageTags) {
    write-host "Pushing images to Docker Hub."
    foreach ($tag in $imageTags) {
        write-host "Push $tag"
        docker push "$tag"
        CheckLastExitCode
    }
}

function PushImagesDryRun($imageTags) {
    write-host "DRY RUN: Pushing images."
    foreach ($tag in $imageTags) {
        write-host "DRY RUN: docker push $tag"
    }
}

function PushImages($imageTags) {
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
$tags = GetUbuntuImageTags $Repo $version $Arch
PushImages $tags

if ($RemoveImages) {
    RemoveImages $tags
}
