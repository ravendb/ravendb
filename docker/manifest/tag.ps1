param([string]$DockerUser, [string]$DockerPass)

if($DockerUser -ne $null -and $DockerPass -ne $null) {
    ./manifest-tool.exe --username $DockerUser --password $DockerPass push from-spec .\manifest.yml;
} else {
    ./manifest-tool.exe push from-spec .\manifest.yml;
}
