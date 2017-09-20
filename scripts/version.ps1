function SetTeamCityEnvironmentVariable ( $name, $value ) {
    Write-Host "##teamcity[setParameter name='$name' value='$value']"
}

function SetVersionEnvironmentVariableInTeamCity($version) {
    SetTeamCityEnvironmentVariable 'env.informationalVersion' $version
}

function SetBuiltAtEnvironmentVariableInTeamCity($builtAt) {
    SetTeamCityEnvironmentVariable 'env.builtAt' $($builtAt.ToString('o'))
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

function GetBuiltAt() {
    return [DateTime]::UtcNow
}

function GetVersion() {
    $buildNumber = GetBuildNumber
    $buildType = GetBuildType
    $builtAt = GetBuiltAt

    if ($buildType.ToLower() -eq 'nightly') {
        $nightlyDateSuffix = $builtAt.ToString("yyyyMMdd-HHmm")
        $versionSuffix = "$buildType-$nightlyDateSuffix"
    } else {
        $versionSuffix = "$buildType-$buildNumber"
    }

    # TODO @gregolsky create a function for this
    # - stable does not have label
    # - increment stable version

    $version = "4.0.0-$versionSuffix"

    SetVersionEnvironmentVariableInTeamCity $version
    SetBuiltAtEnvironmentVariableInTeamCity $builtAt
    
    return @{ 
        Version = $version;
        VersionSuffix = $versionSuffix;
        BuildNumber = $buildNumber;
    }
}
