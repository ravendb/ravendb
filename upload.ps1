$ARTIFACTS = [io.path]::combine("artifacts", "*.zip")

. '.\scripts\env.ps1'
. '.\scripts\checkLastExitCode.ps1'
. '.\scripts\upload.ps1'
. '.\scripts\getScriptDirectory.ps1'

$projectDir = Get-ScriptDirectory
$uploader = [io.path]::combine($projectDir, '..', 'Uploader', 'S3Uploader.exe')
$buildNumber = GetBuildNumber
$files = Get-ChildItem $ARTIFACTS
Upload $UPLOADER $buildNumber $files
