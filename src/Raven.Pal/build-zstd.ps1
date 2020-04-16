$ErrorActionPreference = 'Stop'

if ((Test-Path artifacts) -eq $false) {
    New-Item -Type Directory artifacts
}

$artifactsPath = Resolve-Path artifacts

Push-Location build-libs

try {
    docker build -t build_zstd -f zstd-build.Dockerfile .
    if ($LASTEXITCODE -ne 0) {
        throw "DOCKER BUILD FAILED."
    }

    docker run -it -v "$($artifactsPath):/build/artifacts" build_zstd
    if ($LASTEXITCODE -ne 0) {
        throw "DOCKER BUILD FAILED."
    }
} finally {
    Pop-Location
}
