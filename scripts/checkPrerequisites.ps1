function CheckPrerequisites () {

    if ($(Get-Command "npm" -ErrorAction SilentlyContinue) -eq $null) {
        throw "NPM not found in path."
    }

    if ($(Get-Command "git" -ErrorAction SilentlyContinue) -eq $null) {
        throw "git not found in path."
    }

    if ($(Get-Command "dotnet" -ErrorAction SilentlyContinue) -eq $null) {
        throw "dotnet not found in path."
    }

    if ($(Get-Command "node" -ErrorAction SilentlyContinue) -eq $null) {
        throw "Node.js not found in path."
    }

    $nodeVersion = node --version
    CheckLastExitCode

    if ($($nodeVersion -match '^v?[67]') -eq $False) {
        throw "Incompatible Node.js version. Must be 6 or later."
    }

    if ($PSVersionTable.PSVersion.Major -lt 5) {
        throw "Incompatible Powershell version. Must be 5 or later."
    }
}
