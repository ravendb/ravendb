param(
    $Repo = "ravendb/ravendb",
    $ArtifactsDir = "..\artifacts",
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

function RemoveImages($imageTags) {
    write-host "Removing images."
    foreach ($tag in $imageTags) {
        write-host "Remove $tag"
        docker rmi "$tag"
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
    } else {
        PushImagesDryRun $imageTags
    }
}
function GetImageTags($repo, $version) {
        return @(
            #"$($repo):windows-nanoserver-latest",
            "$($repo):4.2-windows-nanoserver-latest",
            "$($repo):$($version)-windows-nanoserver"
        )
}

$version = GetVersionFromArtifactName
$tags = GetImageTags $Repo $version
PushImages $tags

if ($RemoveImages) {
    RemoveImages $tags
}
