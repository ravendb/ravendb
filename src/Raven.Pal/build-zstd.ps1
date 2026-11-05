$ErrorActionPreference = 'Stop'

if ((Test-Path artifacts) -eq $false) {
    New-Item -Type Directory artifacts
}

$artifactsPath = Resolve-Path artifacts

Push-Location build-libs

try {
    docker build -t build_zstd -f zstd-build.Dockerfile .
    if ($LASTEXITCODE -ne 0) {
        throw "DOCKER BUILD FAILED."
    }

    docker run -it -v "$($artifactsPath):/build/artifacts" build_zstd
    if ($LASTEXITCODE -ne 0) {
        throw "DOCKER BUILD FAILED."
    }
} finally {
    Pop-Location
}

# The mingw toolchain used by build-libs/zstd.sh cannot target Windows ARM64, so this one is built with MSVC.
# Requires Visual Studio 2026 with the 'MSVC ARM64 build tools' and 'C++ CMake tools for Windows' components.

$vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
$vsPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.ARM64 -property installationPath
if ([string]::IsNullOrEmpty($vsPath)) {
    throw "Visual Studio with 'MSVC ARM64 build tools' component was not found."
}

$cmake = Join-Path $vsPath "Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
if ((Test-Path $cmake) -eq $false) {
    throw "CMake was not found at '$cmake'. Install 'C++ CMake tools for Windows' component."
}

$workDir = Join-Path $artifactsPath "zstd-win-arm64"
if (Test-Path $workDir) {
    Remove-Item -Recurse -Force $workDir
}

git clone --branch ravendb --single-branch https://github.com/ravendb/zstd.git (Join-Path $workDir "zstd")
if ($LASTEXITCODE -ne 0) {
    throw "Failed to clone zstd."
}

$buildDir = Join-Path $workDir "build"

& $cmake -S (Join-Path $workDir "zstd\build\cmake") -B $buildDir -G "Visual Studio 18 2026" -A ARM64 `
    -DCMAKE_POLICY_VERSION_MINIMUM="3.5" `
    -DZSTD_MULTITHREAD_SUPPORT=OFF `
    -DZSTD_BUILD_SHARED=ON `
    -DZSTD_BUILD_STATIC=OFF `
    -DZSTD_BUILD_PROGRAMS=OFF `
    -DZSTD_BUILD_TESTS=OFF `
    -DZSTD_USE_STATIC_RUNTIME=ON
if ($LASTEXITCODE -ne 0) {
    throw "CMake configure failed."
}

& $cmake --build $buildDir --config Release
if ($LASTEXITCODE -ne 0) {
    throw "CMake build failed."
}

Copy-Item (Join-Path $buildDir "lib\Release\zstd.dll") (Join-Path $artifactsPath "libzstd.win.arm64.dll")
Remove-Item -Recurse -Force $workDir
