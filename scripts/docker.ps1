function LayoutDockerPrerequisites($projectDir, $artifactsDir) {
    $dockerAssetsDir = Join-Path -Path $projectDir -ChildPath 'docker'

    $artifactsDockerDir = Join-Path -Path $artifactsDir -ChildPath 'docker'
    write-host "Prepare Docker images prerequisites in $artifactsDockerDir.."

    New-Item -ItemType Directory -Path $artifactsDockerDir -Force | Out-Null
    
    $assets = @(
        'ravendb-ubuntu', 
        'ravendb-nanoserver', 
        'build-nanoserver.ps1',
        'build-ubuntu.ps1',
        'publish-nanoserver.ps1',
        'publish-ubuntu.ps1',
        'common.ps1'
    )

    foreach ($asset in $assets) {
        $assetSrcPath = Join-Path -Path $dockerAssetsDir -ChildPath $asset
        write-host "Copy $assetSrcPath -> $artifactsDockerDir"
        Copy-Item -Force -Recurse -Path $assetSrcPath -Destination $artifactsDockerDir
    }

    $settingsFiles = "src\Raven.Server\Properties\Settings\settings.docker.*.json"
    Copy-Item -Force -Path $(Join-Path -Path $projectDir -ChildPath $settingsFiles) -Destination $artifactsDockerDir
}
