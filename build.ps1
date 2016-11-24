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
$RELEASE_DIR = "$PROJECT_DIR\artifacts";
$OUT_DIR = "$PROJECT_DIR\out"
$BUILD_DIR = "$PROJECT_DIR\build"
$BUILD_TOOLS_DIR = "$PROJECT_DIR\build\tools";
$SERVER_SRC_DIR = "$PROJECT_DIR\src\Raven.Server"
$CLIENT_SRC_DIR = "$PROJECT_DIR\src\Raven.Client"
$STUDIO_SRC_DIR = "$PROJECT_DIR\src\Raven.Studio"
$TYPINGS_GENERATOR_SRC_DIR = "$PROJECT_DIR\tools\TypingsGenerator"
$TEMP_DIR = "$PROJECT_DIR\temp"
$RELEASE_ZIP_FILE = "RavenDB-$version.zip"

CleanBuildDirectories $RELEASE_DIR $OUT_DIR $BUILD_DIR

#DownloadDependencies
BuildServer $SERVER_SRC_DIR $OUT_DIR $BUILD_DIR
BuildClient $CLIENT_SRC_DIR $OUT_DIR $BUILD_DIR
BuildTypingsGenerator $TYPINGS_GENERATOR_SRC_DIR
BuildStudio $STUDIO_SRC_DIR $PROJECT_DIR $version

CopyLicenseFile $OUT_DIR
CopyAckFile $OUT_DIR

$STUDIO_BUILD_DIR = "src\Raven.Studio\build";
CopyStudioPackage $STUDIO_BUILD_DIR $OUT_DIR

$RELEASE_ZIP_PATH = "$RELEASE_DIR\$RELEASE_ZIP_FILE"
ZipFilesFromDir $RELEASE_ZIP_PATH "$OUT_DIR"
