$ErrorActionPreference = "Stop"

$DEV_BUILD_NUMBER = 40

. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\checkPrerequisites.ps1'
. '.\scripts\restore.ps1'
. '.\scripts\clean.ps1'
. '.\scripts\package.ps1'
. '.\scripts\buildProjects.ps1'
. '.\scripts\getScriptDirectory.ps1'
. '.\scripts\copyStudioPkg.ps1'
. '.\scripts\copyDocs.ps1'
. '.\scripts\env.ps1'
. '.\scripts\updateSourceWithBuildInfo.ps1'

CheckPrerequisites

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
$NEW_CLIENT_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.NewClient")
$STUDIO_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio")
$TYPINGS_GENERATOR_SRC_DIR = [io.path]::combine($PROJECT_DIR, "tools", "TypingsGenerator")
$STUDIO_BUILD_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio", "build");

$SPECS = @(
    @{
        "Name" = "windows-x64";
        "Runtime" = "win10-x64";
        "PkgType" = "zip";
        "IsUnix" = $False;
    },
    @{
        "Name" = "ubuntu.14.04-x64";
        "Runtime" = "ubuntu.14.04-x64";
        "PkgType" = "tar";
        "IsUnix" = $True;
    },
    @{
        "Name" = "ubuntu.16.04-x64";
        "Runtime" = "ubuntu.16.04-x64";
        "PkgType" = "tar";
        "IsUnix" = $True;
    },
    @{
        "Name" = "raspberry-pi";
        "Runtime" = "ubuntu.16.04-x64";
        "PkgType" = "tar";
        "IsUnix" = $True;
    }
);

CleanBuildDirectories $RELEASE_DIR
DownloadDependencies
BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
BuildStudio $STUDIO_SRC_DIR $PROJECT_DIR $version
UpdateSourceWithBuildInfo $PROJECT_DIR $buildNumber $version

Foreach ($spec in $SPECS) {
    $runtime = $spec.Runtime
    $specOutDir = [io.path]::combine($OUT_DIR, $spec.Name)
    BuildServer $SERVER_SRC_DIR $specOutDir $runtime $spec.Name
    BuildClient $CLIENT_SRC_DIR $specOutDir $BUILD_DIR $spec.Name
    BuildNewClient $NEW_CLIENT_SRC_DIR $specOutDir $BUILD_DIR $spec.Name
    CopyStudioPackage $STUDIO_BUILD_DIR $specOutDir
    CreateRavenPackage $PROJECT_DIR $RELEASE_DIR $specOutDir $version $spec
}

write-host "Done creating packages."
