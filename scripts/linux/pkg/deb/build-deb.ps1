$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrEmpty($env:OUTPUT_DIR)) {
    $env:OUTPUT_DIR = Join-Path $(pwd).Path -ChildPath dist
}

$REQUIRED_VARS = @( 
    "DISTRO_NAME" 
    "DISTRO_VERSION" 
    "DISTRO_VERSION_NAME" 
    "RAVENDB_VERSION"
    "DOTNET_DEPS_VERSION"
    "DOTNET_RUNTIME_VERSION"
)

foreach ($envVar in $REQUIRED_VARS) {
    if (-not (Test-Path "env:$envVar")) {
        throw "Setting $envVar is mandatory."
    }
}

$ravenVersion = $env:RAVENDB_VERSION

$distName = $env:DISTRO_NAME
$distVer = $env:DISTRO_VERSION
$distVerName = $env:DISTRO_VERSION_NAME
$outputDir = $env:OUTPUT_DIR 

$debDotnetRuntimeDepsLine = "dotnet-runtime-deps-$($env:DOTNET_RUNTIME_VERSION) (>= $env:DOTNET_DEPS_VERSION)"

Write-Host "Build DEB of RavenDB $ravenVersion for $distName $distVer $distVerName"

$DOCKER_FILE = "./ubuntu.Dockerfile"
$DEB_BUILD_ENV_IMAGE = "ravendb-deb_ubuntu_$distVerName"

docker build `
    --build-arg "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    --build-arg "DISTRO_VERSION=$env:DISTRO_VERSION" `
    -t $DEB_BUILD_ENV_IMAGE `
    -f $DOCKER_FILE .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the DEB build environment image."
    exit $LASTEXITCODE
}

if (-not (Test-Path $PSScriptRoot/temp)) {
    mkdir $PSScriptRoot/temp
}

$distroOutputDir = Join-Path $env:OUTPUT_DIR -ChildPath "$env:DISTRO_VERSION"
if (-not (Test-Path $distroOutputDir)) {
    mkdir $distroOutputDir
}

docker run --rm -it `
    -v "$($env:OUTPUT_DIR):/dist" `
    -v "$PSScriptRoot/temp:/cache" `
    -e RAVENDB_VERSION=$ravenVersion  `
    -e "DOTNET_RUNTIME_VERSION=$env:DOTNET_RUNTIME_VERSION" `
    -e "DOTNET_DEPS_VERSION=$env:DOTNET_DEPS_VERSION" `
    -e "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    $DEB_BUILD_ENV_IMAGE 

