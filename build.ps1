param(
    $TargetPlatform="",
    [switch]$Windows,
    [switch]$Ubuntu14,
    [switch]$Ubuntu16,
    [switch]$Rpi,
    [switch]$DontRebuildStudio,
    [switch]$Help)

if ($Help) {
    Write-Host -NoNewline -ForegroundColor Cyan "-Windows "
    Write-Host " - build only Windows artifacts"

    Write-Host -NoNewline -ForegroundColor Cyan "-Ubuntu14"
    Write-Host " - build only Ubuntu 14.04 artifacts"
    
    Write-Host -NoNewline -ForegroundColor Cyan "-Ubuntu16"
    Write-Host " - build only Ubuntu 16.04 artifacts"
    
    Write-Host -NoNewline -ForegroundColor Cyan "-Rpi"
    Write-Host " - build only Raspberry Pie artifacts"

    Write-Host -NoNewline -ForegroundColor Cyan "-DontRebuildStudio"
    Write-Host " - skip building studio if it was build before"

    exit 0
}

$ErrorActionPreference = "Stop"

$DEV_BUILD_NUMBER = 40

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
. '.\scripts\platform.ps1'

CheckPrerequisites

$buildNumber = GetBuildNumber
$buildType = GetBuildType

# TODO @gregolsky create a function for this - stable does not have label
$versionSuffix = "$buildType-$buildNumber"
$version = "4.0.0-$versionSuffix"

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

if ($Windows) {
    $TargetPlatform = "windows";
}

if ($Ubuntu14) {
    $TargetPlatform = "ubuntu14";
} 

if ($Ubuntu16) {
    $TargetPlatform = "ubuntu16";
}

if ($Rpi) {
    $TargetPlatform = "rpi";
}

$specs = GetTargetPlatforms $TargetPlatform

SetVersionEnvironmentVariableInTeamCity $version

CleanBuildDirectories $RELEASE_DIR
CleanBuildDirectories $TYPINGS_GENERATOR_BIN_DIR

DownloadDependencies

UpdateSourceWithBuildInfo $PROJECT_DIR $buildNumber $version

BuildSparrow $SPARROW_SRC_DIR

BuildClient $CLIENT_SRC_DIR $CLIENT_OUT_DIR $spec.Name

CreateNugetPackage $CLIENT_SRC_DIR $RELEASE_DIR $versionSuffix

if (ShouldBuildStudio $STUDIO_OUT_DIR $DontRebuildStudio) {
    BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
    BuildStudio $STUDIO_SRC_DIR $version
} else {
    write-host "Not rebuilding studio..."
}

Foreach ($spec in $SPECS) {
    $runtime = $spec.Runtime
    $specOutDir = [io.path]::combine($OUT_DIR, $spec.Name)

    BuildServer $SERVER_SRC_DIR $specOutDir $runtime $spec.Name

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
