function SetTeamCityEnvironmentVariable ( $name, $value ) {
    Write-Host "##teamcity[setParameter name='$name' value='$value']"
}

function SetVersionEnvironmentVariableInTeamCity($version) {
    SetTeamCityEnvironmentVariable 'env.informationalVersion' $version
}

function SetBuiltAtEnvironmentVariableInTeamCity($builtAt) {
    SetTeamCityEnvironmentVariable 'env.BUILT_AT' $($builtAt.ToString('o'))
}

$DEV_BUILD_NUMBER = 40
function GetBuildNumber () {
    if ($env:BUILD_NUMBER) {
        $result = $env:BUILD_NUMBER
    } else {
        $result = $DEV_BUILD_NUMBER
    }

    $result
}

function GetBuildType () {
    if ($env:BUILD_TYPE) {
        $result = $env:BUILD_TYPE
    } else {
        $result = "custom";
    }

    $result
}

$RELEASE_INFO_FILE = 'artifacts/release-info.json'
function SetVersionInfo() {
    $buildNumber = GetBuildNumber
    $buildType = GetBuildType
    $builtAt = [DateTime]::UtcNow
    $builtAtString = $builtAt.ToString("yyyyMMdd-HHmm")

    if ($buildType.ToLower() -eq 'nightly') {
        $versionSuffix = "$buildType-$builtAtString"
        $buildNumber = $DEV_BUILD_NUMBER
    } else {
        $versionSuffix = "$buildType-$buildNumber"
    }

    # TODO @gregolsky create a function for this
    # - stable does not have label
    # - increment stable version

    $version = "4.0.0-$versionSuffix"

    SetVersionEnvironmentVariableInTeamCity $version
    SetBuiltAtEnvironmentVariableInTeamCity $builtAt
    
    $versionInfo = @{ 
        Version = $version;
        VersionSuffix = $versionSuffix;
        BuildNumber = $buildNumber;
        BuiltAt = $builtAt;
        BuiltAtString = $builtAtString;
        BuildType = $buildType
    }

    New-Item -Path $RELEASE_INFO_FILE -Force -Type File
    $versionInfoJson = ConvertTo-Json -InputObject $versionInfo
    Set-Content -Path $RELEASE_INFO_FILE -Value $versionInfoJson

    return $versionInfo
}

function GetVersionInfo() {
    return Get-Content -Path $RELEASE_INFO_FILE | ConvertFrom-Json
}
