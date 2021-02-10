Write-Host "Test DEB of RavenDB $env:RAVENDB_VERSION on $env:DISTRO_VERSION"

$DOCKER_FILE = "./ubuntu_test.Dockerfile"
$DEB_TEST_ENV_IMAGE = "ravendb-deb_test_ubuntu-$env:DISTRO_VERSION_NAME"

docker build `
    --build-arg "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    --build-arg "DISTRO_VERSION=$env:DISTRO_VERSION" `
    -t $DEB_TEST_ENV_IMAGE `
    -f $DOCKER_FILE .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the DEB build environment image."
    exit $LASTEXITCODE
}

$pkgPath = (Get-ChildItem "dist/$env:DISTRO_VERSION/ravendb_$env:RAVENDB_VERSION*.deb").Name

docker run --rm -it `
    -v "$($env:OUTPUT_DIR):/dist" `
    -e PACKAGE_PATH=/dist/$env:DISTRO_VERSION/$pkgPath `
    -e OUTPUT_DIR=$($env:OUTPUT_DIR) `
    $DEB_TEST_ENV_IMAGE 
