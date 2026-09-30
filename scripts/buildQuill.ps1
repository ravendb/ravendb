$QUILL_PNPM_VERSION = "11.24.0"

$QUILL_RAVENDB_PROJECTS = @(
    "src/Raven.Server/Raven.Server.csproj",
    "tools/rvn/rvn.csproj",
    "tools/Raven.Debug/Raven.Debug.csproj",
    "tools/Voron.Recovery/Voron.Recovery.csproj",
    "tools/Raven.Migrator/Raven.Migrator.csproj"
)

function PublishFrameworkDependent ( $projectDir, $csproj, $runtime, $outDir, $extraArgs = @() ) {
    write-host "Publishing $csproj ($runtime) -> $outDir"
    Push-Location $projectDir
    try {
        exec { dotnet publish $csproj `
            --configuration Release `
            --runtime $runtime `
            --self-contained false `
            --output $outDir `
            --nologo `
            @extraArgs }
        CheckLastExitCode
    }
    finally {
        Pop-Location
    }
}

function BuildQuillWeb ( $projectDir, $webOutDir ) {
    $quillProj = "src/Raven.Quill/Raven.Quill.csproj"
    $webSrcDir = [io.path]::combine($projectDir, "src", "Raven.Quill.Web")

    # OpenAPI spec the web bundle generates its client from
    write-host "Generating Quill OpenAPI spec..."
    Push-Location $projectDir
    try {
        exec { dotnet build $quillProj --configuration Release --nologo --tl:off }
        CheckLastExitCode
    }
    finally {
        Pop-Location
    }

    # build the web bundle (quill-frontend-build stage)
    write-host "Building Quill web bundle..."
    Push-Location $webSrcDir
    try {
        if ($null -eq (Get-Command pnpm -ErrorAction SilentlyContinue)) {
            write-host "pnpm not found on PATH; bootstrapping via corepack..."
            # Don't prompt for the pnpm download — would hang a non-interactive/CI build.
            $env:COREPACK_ENABLE_DOWNLOAD_PROMPT = "0"
            exec { corepack enable }
            CheckLastExitCode
            exec { corepack prepare "pnpm@$QUILL_PNPM_VERSION" --activate }
            CheckLastExitCode
        }

        $env:CI = "1"
        exec { pnpm install --frozen-lockfile }
        CheckLastExitCode

        $env:RAVEN_QUILL_OPENAPI_SKIP_SPEC_BUILD = "1"
        exec { pnpm build }
        CheckLastExitCode
    }
    finally {
        Remove-Item Env:\RAVEN_QUILL_OPENAPI_SKIP_SPEC_BUILD -ErrorAction SilentlyContinue
        Pop-Location
    }
}

function BuildQuill ( $projectDir, $target, $studioZipPath, $stagingDir ) {
    write-host "Building Quill payload for $($target.Name)..."

    $ravendbOut = [io.path]::combine($stagingDir, "ravendb")
    $webOut = [io.path]::combine($stagingDir, "web")

    if (Test-Path $stagingDir) { Remove-Item -Recurse -Force $stagingDir }
    New-Item -ItemType Directory -Path $ravendbOut -Force | Out-Null
    New-Item -ItemType Directory -Path $webOut -Force | Out-Null

    # ravendb server + tools to ravendb/
    foreach ($csproj in $QUILL_RAVENDB_PROJECTS) {
        PublishFrameworkDependent $projectDir $csproj $target.Runtime $ravendbOut
    }

    # reuse the Studio.zip build.ps1 already produced
    if (-not (Test-Path $studioZipPath)) {
        throw "Raven.Studio.zip not found at $studioZipPath (build Studio before -Quill)."
    }
    Copy-Item -Force $studioZipPath ([io.path]::combine($ravendbOut, "Raven.Studio.zip"))

    BuildQuillWeb $projectDir $webOut
    PublishFrameworkDependent $projectDir "src/Raven.Quill/Raven.Quill.csproj" $target.Runtime $webOut @("-p:OpenApiGenerateDocuments=false")

    $wwwroot = [io.path]::combine($webOut, "wwwroot")
    if (Test-Path $wwwroot) { Remove-Item -Recurse -Force $wwwroot }
    $webDist = [io.path]::combine($projectDir, "src", "Raven.Quill.Web", "dist")
    Copy-Item -Recurse -Force $webDist $wwwroot

    return $stagingDir
}

function CreateQuillPackage ( $releaseDir, $version, $target, $stagingDir ) {
    # Base name only, no extension: CreateArchiveFromDir/TarBzFilesFromDir appends .tar.bz2
    # (matching RavenDB's GetRavenArchiveFileName), so passing .tar.bz2 here would double it.
    $archiveBaseName = "Quill-$version-$($target.Name)"
    $archiveBasePath = [io.path]::combine($releaseDir, $archiveBaseName)
    write-host "Creating Quill package $archiveBasePath.tar.bz2"
    CreateArchiveFromDir $archiveBasePath $stagingDir $target "Quill"
}
