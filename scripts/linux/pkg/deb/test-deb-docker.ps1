Write-Host "Test DEB of RavenDB $env:RAVENDB_VERSION on $env:DISTRO_VERSION $env:DEB_ARCHITECTURE"

if ([string]::IsNullOrEmpty($env:OUTPUT_DIR)) {
    $env:OUTPUT_DIR = Join-Path $PSScriptRoot -ChildPath dist
}

$distroOutputDir = Join-Path $env:OUTPUT_DIR -ChildPath "$env:DISTRO_VERSION"
$pkgFilter = "ravendb_$($env:RAVENDB_VERSION)-*_ubuntu.$($env:DISTRO_VERSION)_$($env:DEB_ARCHITECTURE).deb"

$pkgCandidates = @(
    Get-ChildItem $distroOutputDir -Filter $pkgFilter -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending
)

if ($pkgCandidates.Count -eq 0) {
    Write-Host "No .deb matching '$pkgFilter' found in '$distroOutputDir'. Build the package first."
    exit 1
}

if ($pkgCandidates.Count -gt 1) {
    Write-Host "Found $($pkgCandidates.Count) .deb files matching '$pkgFilter'; using the most recent: $($pkgCandidates[0].Name)"
}

$pkgPath = $pkgCandidates[0].Name

$DOCKER_FILE = "./ubuntu_test.Dockerfile"
$DEB_TEST_ENV_IMAGE = "ravendb-deb_test_ubuntu-$($env:DISTRO_VERSION_NAME)_$($env:DEB_ARCHITECTURE)"

docker build `
    --platform $env:DOCKER_BUILDPLATFORM `
    --build-arg "DISTRO_VERSION_NAME=$env:DISTRO_VERSION_NAME" `
    --build-arg "DISTRO_VERSION=$env:DISTRO_VERSION" `
    -t $DEB_TEST_ENV_IMAGE `
    -f $DOCKER_FILE .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the DEB test environment image."
    exit $LASTEXITCODE
}

# ubuntu_test.Dockerfile needs -t because its CMD drops to an interactive bash when the package
# test fails. -i must be omitted when stdin is not a console, otherwise docker aborts with
# "the input device is not a TTY" before the package is ever installed.
$dockerRunArgs = @("--rm", "-t")
$dockerCmdArgs = @()

if (-not [Console]::IsInputRedirected) {
    $dockerRunArgs += "-i"
} else {
    # Non-interactive: run the test directly instead of the image CMD. That CMD ends in
    # "|| (apt install vim less; bash)", and with no stdin attached the fallback bash reads EOF
    # and exits 0 - which would report success for a package test that actually failed.
    $dockerCmdArgs = @("bash", "-c", 'source /assets/test.sh && test_package_local $PACKAGE_PATH')
}

docker run @dockerRunArgs `
    --platform $env:DOCKER_BUILDPLATFORM `
    -v "$($env:OUTPUT_DIR):/dist" `
    -e PACKAGE_PATH=/dist/$env:DISTRO_VERSION/$pkgPath `
    $DEB_TEST_ENV_IMAGE @dockerCmdArgs

if ($LASTEXITCODE -ne 0) {
    Write-Host "DEB package test failed."
    exit $LASTEXITCODE
}
