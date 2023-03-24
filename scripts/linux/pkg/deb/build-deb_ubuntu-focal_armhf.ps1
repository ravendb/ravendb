if (-not $env:OUTPUT_DIR) {
    $env:OUTPUT_DIR = "$PSScriptRoot/dist"
}

.\set-ubuntu-focal.ps1
.\set-raven-platform-armhf.ps1
.\set-raven-version-env.ps1

.\build-deb.ps1
