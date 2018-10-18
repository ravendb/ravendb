$ErrorActionPreference = 'Stop'

function Get-ScriptDirectory {
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value
    Split-Path $Invocation.MyCommand.Path
}

function CheckLastExitCode {
    param ([int[]]$SuccessCodes = @(0), [scriptblock]$CleanupScript = $null)

    if ($SuccessCodes -notcontains $LastExitCode) {
        if ($CleanupScript) {
            "Executing cleanup script: $CleanupScript"
            &$CleanupScript
        }
        $msg = @"
EXE RETURNED EXIT CODE $LastExitCode
CALLSTACK:$(Get-PSCallStack | Out-String)
"@
        throw $msg
    }
}

$scriptDir = Get-ScriptDirectory

$projectDir = [io.path]::combine($scriptDir, "..\..")
$dockerDir = [io.path]::combine($projectDir, "docker")
$debugDockerDir = [io.path]::combine($projectDir, "docker", "debug")
$debugDockerfileDir = [io.path]::combine($projectDir, "docker", "debug", "ravendb-linux-debug")

$pkgFile = [io.path]::combine($projectDir, "artifacts", "RavenDB-4.2.0-custom-42-linux-x64.tar.bz2")

if ($(Test-Path $pkgFile) -eq $False) {

    Push-Location .

    try {
        Set-Location $projectDir
        .\build.ps1 -LinuxX64 -Debug
        CheckLastExitCode
        write-host "Built RavenDB in Debug"
    }
    finally {
        Pop-Location
    }

} else {
    write-host "Found RavenDB server in artifacts. Building image with whatever is in $pkgFile."
}

Push-Location .
try {
    Set-Location $dockerDir
    .\build-ubuntu.ps1
    CheckLastExitCode
    write-host "Built RavenDB Linux docker image."
}
finally {
    Pop-Location 
}

Push-Location .

$dockerDebugTag = "ravendb/ravendb:ubuntu-debug"

try {
    Set-Location $debugDockerDir
    docker build -t "$dockerDebugTag" "$debugDockerfileDir"
    CheckLastExitCode
}
finally {
    Pop-Location 
}

write-host "Built docker debug image $dockerDebugTag"

