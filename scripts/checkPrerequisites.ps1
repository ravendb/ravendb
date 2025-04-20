function CheckPrerequisites () {

    if ($null -eq $(Get-Command "npm" -ErrorAction SilentlyContinue)) {
        throw "NPM not found in path."
    }

    if ($null -eq $(Get-Command "git" -ErrorAction SilentlyContinue)) {
        throw "git not found in path."
    }

    if ($null -eq $(Get-Command "dotnet" -ErrorAction SilentlyContinue)) {
        throw "dotnet not found in path."
    }

    if ($null -eq $(Get-Command "node" -ErrorAction SilentlyContinue)) {
        throw "Node.js not found in path."
    }

    $nodeVersion = node --version
    CheckLastExitCode

    if ($($nodeVersion -match '^v?2(0|2)') -eq $False) {
        throw "Incompatible Node.js version. Only versions 20 or 22 are supported."
    }

    if ($PSVersionTable.PSVersion.Major -lt 5) {
        throw "Incompatible Powershell version. Must be 5 or later."
    }
}
