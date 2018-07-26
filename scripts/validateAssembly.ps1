function Assert-AssemblyConfiguration () {
    # TODO
    param(
        $AssemblyPath,
        $ExpectedConfiguration)

    $assembly = [System.Reflection.Assembly]::LoadFrom($AssemblyPath)
    $assemblyConfAttributes = $assembly.GetCustomAttributes();
    write-host $assemblyConfAttributes
    if ($ExpectedConfiguration -ne $assemblyConfAttributes[0].Configuration) {
        throw "Invalid assembly configuration. Expected: $ExpectedConfiguration Actual: $($assemblyConfAttributes.Configuration)"
    }
}

function Assert-AssemblyFileVersion() {
    param(
        $AssemblyPath,
        $ExpectedFileVersion)
    $info = [System.Diagnostics.FileVersionInfo]::GetVersionInfo($AssemblyPath)
    if ($ExpectedFileVersion -ne $info.FileVersion) {
        throw "Invalid assembly file version. Expected: $ExpectedFileVersion Actual: $($info.FileVersion)"
    }

    write-host "File version ($AssemblyPath): $ExpectedFileVersion"
}

function Assert-AssemblyProductVersion() {
    param(
        $AssemblyPath,
        $ExpectedProductVersion)

    $info = [System.Diagnostics.FileVersionInfo]::GetVersionInfo($AssemblyPath)
    if ($ExpectedProductVersion -ne $info.ProductVersion) {
        throw "Invalid assembly file version. Expected: $ExpectedProductVersion Actual: $($info.ProductVersion)"
    }

    write-host "Product version ($AssemblyPath): $ExpectedProductVersion"
}

function Assert-AssemblyVersion () {
    param(
        $AssemblyPath,
        $ExpectedVersion)

    if (!(Test-Path $AssemblyPath)) {
        throw "Assembly file $AssemblyPath does not exist."
    }

    if ($IsWindows -eq $False) {
        return
    }

    $cmd = '[string]::Join(".", $([System.Reflection.Assembly]::LoadFrom("' + $AssemblyPath.Replace("\", "\\") + '").GetName().Version.ToString().Split(".") | Select-Object -First 3));'
    $bytes = [System.Text.Encoding]::Unicode.GetBytes($cmd)
    $encodedCommand = [Convert]::ToBase64String($bytes)

    # Need to run this in another AppDomain, otherwise it locks the assembly
    # Hence we run that on another PS process instance
    $pwsh = if ($IsWindows -eq $False) { "pwsh" } else { "Powershell" }
    $assemblyVersion = & "$pwsh" -EncodedCommand $encodedCommand
    if ($ExpectedVersion -ne $assemblyVersion) {
        throw "Invalid assembly version. Expected: $ExpectedVersion Actual: $assemblyVersion"
    }

    write-host "Assembly version ($AssemblyPath): $ExpectedVersion"
}
