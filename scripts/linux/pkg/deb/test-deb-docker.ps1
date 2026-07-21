Write-Host "Test DEB of RavenDB $env:RAVENDB_VERSION on $env:DISTRO_VERSION $env:DEB_ARCHITECTURE"

$DOCKER_FILE = "./ubuntu_test.Dockerfile"
$DEB_TEST_ENV_IMAGE = "ravendb-deb_test_ubuntu-$($env:DISTRO_VERSION_NAME)_$($env:DEB_ARCHITECTURE)"

docker build `
    --platform $env:DOCKER_BUILDPLATFORM `
    --build-arg "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    --build-arg "DISTRO_VERSION=$env:DISTRO_VERSION" `
    -t $DEB_TEST_ENV_IMAGE `
    -f $DOCKER_FILE .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the DEB build environment image."
    exit $LASTEXITCODE
}

$pkgPath = (Get-ChildItem "dist/$env:DISTRO_VERSION/ravendb_$($env:RAVENDB_VERSION)-*_ubuntu.$($env:DISTRO_VERSION)_$($env:DEB_ARCHITECTURE).deb").Name

if (-not $pkgPath) {
    Write-Host "No .deb found in dist/$env:DISTRO_VERSION for architecture $env:DEB_ARCHITECTURE. Build the package first."
    exit 1
}

docker run --rm -it `
    --platform $env:DOCKER_BUILDPLATFORM `
    -v "$($env:OUTPUT_DIR):/dist" `
    -e PACKAGE_PATH=/dist/$env:DISTRO_VERSION/$pkgPath `
    -e OUTPUT_DIR=$($env:OUTPUT_DIR) `
    $DEB_TEST_ENV_IMAGE 
