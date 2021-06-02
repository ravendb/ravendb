# start a vagrant vm
# install deb
# verify it starts as expected (setup mode etc.)
# remove (check if data and cert are left intact)
# purge (check if everything is gone - service, data, security)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrEmpty($env:OUTPUT_DIR)) {
    $env:OUTPUT_DIR = Join-Path $(pwd).Path -ChildPath dist
}

Write-Host "Test RavenDB $env:RAVENDB_VERSION on Vagrant, $env:DISTRO_NAME $env:DISTRO_VERSION"

$env:PKG_OUTPUT_DIR = Join-Path -Path $(Resolve-Path $env:OUTPUT_DIR) -ChildPath $env:DISTRO_VERSION

Push-Location ./vagrant/$($env:DISTRO_VERSION_NAME)
try {
    vagrant up --provider=hyperv 
} finally {
    Pop-Location
}