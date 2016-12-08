$ErrorActionPreference = "Stop"

$DEV_BUILD_NUMBER = 40

. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\checkPrerequisites.ps1'
. '.\scripts\restore.ps1'
. '.\scripts\clean.ps1'
. '.\scripts\package.ps1'
. '.\scripts\buildProjects.ps1'
. '.\scripts\getScriptDirectory.ps1'
. '.\scripts\copyDocs.ps1'
. '.\scripts\env.ps1'
. '.\scripts\updateSourceWithBuildInfo.ps1'
. '.\scripts\nuget.ps1'

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

$NEW_CLIENT_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.NewClient")
$NEW_CLIENT_OUT_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.NewClient", "bin", "Release")

$SERVER_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Server")

$SPARROW_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Sparrow")
$TYPINGS_GENERATOR_SRC_DIR = [io.path]::combine($PROJECT_DIR, "tools", "TypingsGenerator")

$STUDIO_SRC_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio")
$STUDIO_OUT_DIR = [io.path]::combine($PROJECT_DIR, "src", "Raven.Studio", "build")

$SPECS = @(
    @{
        "Name" = "windows-x64";
        "Runtime" = "win10-x64";
        "PkgType" = "zip";
        "IsUnix" = $False;
    }
    @{
       "Name" = "ubuntu.14.04-x64";
       "Runtime" = "ubuntu.14.04-x64";
       "PkgType" = "tar.bz2";
       "IsUnix" = $True;
    },
    @{
       "Name" = "ubuntu.16.04-x64";
       "Runtime" = "ubuntu.16.04-x64";
       "PkgType" = "tar.bz2";
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

#DownloadDependencies

UpdateSourceWithBuildInfo $PROJECT_DIR $buildNumber $version

BuildSparrow $SPARROW_SRC_DIR
BuildClient $CLIENT_SRC_DIR $CLIENT_OUT_DIR $spec.Name
BuildNewClient $NEW_CLIENT_SRC_DIR $NEW_CLIENT_OUT_DIR $spec.Name

CreateNugetPackage $NEW_CLIENT_SRC_DIR $RELEASE_DIR $versionSuffix
CreateNugetPackage $CLIENT_SRC_DIR $RELEASE_DIR $versionSuffix

BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
#BuildStudio $STUDIO_SRC_DIR $PROJECT_DIR $version

Foreach ($spec in $SPECS) {
    $runtime = $spec.Runtime
    $specOutDir = [io.path]::combine($OUT_DIR, $spec.Name)
    BuildServer $SERVER_SRC_DIR $specOutDir $runtime $spec.Name
    

    $specOutDirs = @{
        "Main" = $specOutDir;
        "Client" = $CLIENT_OUT_DIR;
        "NewClient" = $NEW_CLIENT_OUT_DIR;
        "Server" = $([io.path]::combine($specOutDir, "Server"));
        "Studio" = $STUDIO_OUT_DIR
    }

    CreateRavenPackage $PROJECT_DIR $RELEASE_DIR $specOutDirs $spec $version
}

write-host "Done creating packages."
