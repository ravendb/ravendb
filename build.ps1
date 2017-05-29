param(
    $Target="",
    [switch]$WinX64,
    [switch]$WinX86,
    [switch]$Ubuntu14,
    [switch]$Ubuntu16,
    [switch]$Rpi,
    [switch]$DontRebuildStudio,
    [switch]$Help)

$ErrorActionPreference = "Stop"

. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\checkPrerequisites.ps1'
. '.\scripts\restore.ps1'
. '.\scripts\clean.ps1'
. '.\scripts\arm.ps1'
. '.\scripts\archive.ps1'
. '.\scripts\package.ps1'
. '.\scripts\buildProjects.ps1'
. '.\scripts\getScriptDirectory.ps1'
. '.\scripts\copyAssets.ps1'
. '.\scripts\env.ps1'
. '.\scripts\updateSourceWithBuildInfo.ps1'
. '.\scripts\nuget.ps1'
. '.\scripts\target.ps1'
. '.\scripts\help.ps1'

if ($Help) {
    Help
}

CheckPrerequisites

$buildNumber = GetBuildNumber
$buildType = GetBuildType

# TODO @gregolsky create a function for this - stable does not have label
$versionSuffix = "$buildType-$buildNumber"
$version = "4.0.0-$versionSuffix"

Write-Host -ForegroundColor Green "Building $version"

$PROJECT_DIR = Get-ScriptDirectory
$RELEASE_DIR = [io.path]::combine($PROJECT_DIR, "artifacts")
$OUT_DIR = [io.path]::combine($PROJECT_DIR, "artifacts")

$CLIENT_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Client")
$CLIENT_OUT_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Client", "bin", "Release")

$SERVER_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Server")

$SPARROW_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Sparrow")
$SPARROW_OUT_DIR = [io.path]::combine($PROJECT_DIR, "src", "Sparrow", "bin", "Release")

$TYPINGS_GENERATOR_SRC_DIR = [io.path]::combine($PROJECT_DIR, "tools", "TypingsGenerator")
$TYPINGS_GENERATOR_BIN_DIR = [io.path]::combine($TYPINGS_GENERATOR_SRC_DIR, "bin")

$STUDIO_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio")
$STUDIO_OUT_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio", "build")

if ([string]::IsNullOrEmpty($Target) -eq $false) {
    $Target = $Target.Split(",")
} else {
    $Target = $null

    if ($WinX64) {
        $Target = @( "win-x64" );
    }

    if ($WinX86) {
        $Target = @( "win-x86" );
    }

    if ($Ubuntu14) {
        $Target = @( "ubuntu14" );
    } 

    if ($Ubuntu16) {
        $Target = @( "ubuntu16" );
    }

    if ($Rpi) {
        $Target = @( "rpi" );
    }
}

$targets = GetBuildTargets $Target

if ($targets.Count -eq 0) {
    write-host "No targets specified."
    exit 0;
} else {
    Write-Host -ForegroundColor Magenta "Build targets: $($targets.Name)"
}

SetVersionEnvironmentVariableInTeamCity $version

CleanBuildDirectories $RELEASE_DIR
CleanBuildDirectories $TYPINGS_GENERATOR_BIN_DIR

DownloadDependencies

UpdateSourceWithBuildInfo $PROJECT_DIR $buildNumber $version

BuildSparrow $SPARROW_SRC_DIR

BuildClient $CLIENT_SRC_DIR

CreateNugetPackage $CLIENT_SRC_DIR $RELEASE_DIR $versionSuffix

if (ShouldBuildStudio $STUDIO_OUT_DIR $DontRebuildStudio) {
    BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
    BuildStudio $STUDIO_SRC_DIR $version
} else {
    write-host "Not rebuilding studio..."
}

Foreach ($spec in $targets) {
    $specOutDir = [io.path]::combine($OUT_DIR, $spec.Name)

    BuildServer $SERVER_SRC_DIR $specOutDir $spec

    $specOutDirs = @{
        "Main" = $specOutDir;
        "Client" = $CLIENT_OUT_DIR;
        "Server" = $([io.path]::combine($specOutDir, "Server"));
        "Studio" = $STUDIO_OUT_DIR;
        "Sparrow" = $SPARROW_OUT_DIR;
    }

    CreateRavenPackage $PROJECT_DIR $RELEASE_DIR $specOutDirs $spec $version
}

write-host "Done creating packages."
