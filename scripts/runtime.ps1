function AddRuntimeTxt($projectDir, $packageDir) {
    $runtimeTxtPath = Join-Path -Path $packageDir -ChildPath "runtime.txt"
    $dotnetVersion = GetDotnetVersion
    Add-Content -Path $runtimeTxtPath -Value ".NET Core SDK: $dotnetVersion"

    $runtimeVersion = GetRuntimeFxVersion $projectDir
    Add-Content -Path $runtimeTxtPath -Value ".NET Core Runtime: $runtimeVersion"
}

function GetDotnetVersion() {
    $result = $(dotnet --version)
    return $result
}

function GetRuntimeFxVersion($projectDir) {
    $csprojPath = [io.path]::Combine($projectDir, "src", "Raven.Server", "Raven.Server.csproj")
    [xml]$csproj = Get-Content -Path $csprojPath
    return $csproj.Project.PropertyGroup[0].RuntimeFrameworkVersion
}
