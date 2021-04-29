$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrEmpty($env:OUTPUT_DIR)) {
    $env:OUTPUT_DIR = Join-Path $(pwd).Path -ChildPath dist
}

$REQUIRED_VARS = @( 
    "DISTRO_NAME" 
    "DISTRO_VERSION" 
    "DISTRO_VERSION_NAME" 
    "RAVENDB_VERSION",
    "RAVEN_PLATFORM",
    "DOCKER_BUILDPLATFORM"
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

Write-Host "Build DEB of RavenDB $ravenVersion for distro $distName $distVer $distVerName $env:DEB_ARCHITECTURE"

if ($env:DEB_ARCHITECTURE -eq "amd64") {
    $DOCKER_FILE = "./ubuntu_amd64.Dockerfile"
} else {
    $DOCKER_FILE = "./ubuntu_multiarch.Dockerfile"
}

$DEB_BUILD_ENV_IMAGE = "ravendb-deb_ubuntu_$env:DEB_ARCHITECTURE"

docker build `
    --platform $env:DOCKER_BUILDPLATFORM `
    --build-arg "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    --build-arg "DISTRO_VERSION=$env:DISTRO_VERSION" `
    --build-arg "QEMU_ARCH=$env:QEMU_ARCH" `
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
    --platform $env:DOCKER_BUILDPLATFORM `
    -v "$($env:OUTPUT_DIR):/dist" `
    -v "$PSScriptRoot/temp:/cache" `
    -e RAVENDB_VERSION=$ravenVersion  `
    -e "DOTNET_RUNTIME_VERSION=$env:DOTNET_RUNTIME_VERSION" `
    -e "DOTNET_DEPS_VERSION=$env:DOTNET_DEPS_VERSION" `
    -e "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    -e "RAVEN_PLATFORM=$env:RAVEN_PLATFORM" `
    -e "QEMU_ARCH=$env:QEMU_ARCH" `
    $DEB_BUILD_ENV_IMAGE 

