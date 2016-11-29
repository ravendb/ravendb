$ErrorActionPreference = "Stop"

$DEV_BUILD_NUMBER = 40

. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\restore.ps1'
. '.\scripts\clean.ps1'
. '.\scripts\package.ps1'
. '.\scripts\buildProjects.ps1'
. '.\scripts\getScriptDirectory.ps1'
. '.\scripts\copyStudioPkg.ps1'
. '.\scripts\copyDocs.ps1'
. '.\scripts\env.ps1'
. '.\scripts\updateSourceWithBuildInfo.ps1'

$buildNumber = GetBuildNumber
$buildType = GetBuildType

# TODO @gregolsky create a function for this - stable does not have label
$version = "4.0.0-$buildType-$buildNumber"

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

$RUNTIMES = @(
    "win10-x64",
    "ubuntu.14.04-x64",
    "ubuntu.16.04-x64"
);

CleanBuildDirectories $RELEASE_DIR
DownloadDependencies
BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
BuildStudio $STUDIO_SRC_DIR $PROJECT_DIR $version
UpdateSourceWithBuildInfo $PROJECT_DIR $buildNumber $version

Foreach ($runtime in $RUNTIMES) {
    $runtimeOutDir = [io.path]::combine($OUT_DIR, $runtime)
    BuildServer $SERVER_SRC_DIR $runtimeOutDir $runtime
    BuildClient $CLIENT_SRC_DIR $runtimeOutDir $BUILD_DIR $runtime
    CopyStudioPackage $STUDIO_BUILD_DIR $runtimeOutDir
    CreateRavenPackage $PROJECT_DIR $RELEASE_DIR $runtimeOutDir $version $runtime
}

write-host "Done creating packages."
