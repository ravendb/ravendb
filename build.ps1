$ErrorActionPreference = "Stop"

$DEV_BUILD_NUMBER = 40

$buildNumber = -1;
$buildType = ""
$version = ""

if ($env:BUILD_NUMBER) {
    $buildNumber = $env:BUILD_NUMBER
} else {
    $buildNumber = $DEV_BUILD_NUMBER
}

if ($env:BUILD_TYPE) {
    $buildType = $env:BUILD_TYPE
} else {
    $buildType = "custom";
}

# TODO @gregolsky create a function for this - stable does not have label
$version = "4.0.0-$buildType-$buildNumber"

. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\restore.ps1'
. '.\scripts\clean.ps1'
. '.\scripts\zipFiles.ps1'
. '.\scripts\buildProjects.ps1'
. '.\scripts\getScriptDirectory.ps1'
. '.\scripts\copyStudioPkg.ps1'
. '.\scripts\copyLicense.ps1'

$PROJECT_DIR = Get-ScriptDirectory
$RELEASE_DIR = [io.path]::combine($PROJECT_DIR, "artifacts")
$OUT_DIR = [io.path]::combine($PROJECT_DIR, "artifacts")
$BUILD_DIR = [io.path]::combine($PROJECT_DIR, "artifacts", "build")
$BUILD_TOOLS_DIR = [io.path]::combine($PROJECT_DIR, "build", "tools");
$SERVER_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Server")
$CLIENT_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Client")
$STUDIO_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio")
$TYPINGS_GENERATOR_SRC_DIR = [io.path]::combine($PROJECT_DIR, "tools", "TypingsGenerator")
$STUDIO_BUILD_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio", "build");
$TEMP_DIR = [io.path]::combine($PROJECT_DIR, "temp")
$RUNTIMES = @(
    "win10-x64",
    "ubuntu.16.04-x64"
);

CleanBuildDirectories $RELEASE_DIR

DownloadDependencies

BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
BuildStudio $STUDIO_SRC_DIR $PROJECT_DIR $version

Foreach ($runtime in $RUNTIMES) {
    $runtimeOutDir = [io.path]::combine($OUT_DIR, $runtime)
    BuildServer $SERVER_SRC_DIR $runtimeOutDir $runtime
    BuildClient $CLIENT_SRC_DIR $runtimeOutDir $runtime
    CopyStudioPackage $STUDIO_BUILD_DIR $runtimeOutDir
    CopyLicenseFile $runtimeOutDir
    CopyAckFile $runtimeOutDir

    $releaseZipFile = "RavenDB-$version-$runtime.zip"
    $runtimeOutDir = [io.path]::combine($OUT_DIR, $runtime)
    $releaseZipPath = [io.path]::combine($RELEASE_DIR, $releaseZipFile)
    ZipFilesFromDir $releaseZipPath $runtimeOutDir
}
