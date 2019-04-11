function ValidateRuntimeConfig($target, $serverOutDir) {
    $runtimeConfigPath = [io.path]::Combine($serverOutDir, "Raven.Server.runtimeconfig.json")
    if ((Test-Path $runtimeConfigPath) -eq $False) {
        throw "Runtime config file not found for target $($target.TargetId) at $runtimeConfigPath."
    }

    $config = ParseRuntimeConfig $runtimeConfigPath
    if (!(IsValidRuntimeConfig $config $target)) {
        throw "Runtime config not valid for $($target.Name)"
    } 

    Write-Host "Runtime config for $($target.Name) OK"
}

function IsValidRuntimeConfig($config, $target) {
    if ($target.Arch -eq "x86") {
        return ($config.runtimeOptions.configProperties."System.GC.Concurrent" -eq $False) `
            -and ($config.runtimeOptions.configProperties."System.GC.Server" -eq $False) `
            -and ($config.runtimeOptions.configProperties."System.GC.RetainVM" -eq $False);
    }

    return ($config.runtimeOptions.configProperties."System.GC.Concurrent" -eq $True) `
        -and ($config.runtimeOptions.configProperties."System.GC.Server" -eq $True) `
        -and ($config.runtimeOptions.configProperties."System.GC.RetainVM" -eq $True);
}

function ParseRuntimeConfig($runtimeConfigPath) {
    return Get-Content $runtimeConfigPath | ConvertFrom-Json
}
