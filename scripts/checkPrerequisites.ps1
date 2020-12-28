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

    if ($($nodeVersion -match '^v?[6789]|1\d') -eq $False) {
        throw "Incompatible Node.js version. Must be 6 or later."
    }

    if ($PSVersionTable.PSVersion.Major -lt 5) {
        throw "Incompatible Powershell version. Must be 5 or later."
    }
}
