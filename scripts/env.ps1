function SetTeamCityEnvironmentVariable ( $name, $value ) {
    Write-Host "##teamcity[setParameter name='$name' value='$value']"
}

function SetVersionEnvironmentVariableInTeamCity($version) {
    SetTeamCityEnvironmentVariable 'env.informationalVersion' $version
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
