Build Raven.PAL (librvnpal)
===========================

Prereqs:
  - zig on PATH (built with 0.13.0):  winget install -e --id zig.zig  |  https://ziglang.org/download/
  - pwsh, .NET SDK
  - libs/liburing/*.a (already in repo)

Build (from src/Raven.Pal):
  pwsh ./build.ps1                       # bump pal.ver, cross-compile all targets, pack libs/Raven.PAL.7.0.<ver>.nupkg, update Directory.Packages.props
  pwsh ./build.ps1 -skip_version_increment   # rebuild without bumping the version

Then bump PAL_VER in src/Sparrow.Server/Platform/Pal.cs to match pal.ver
(must equal rvn_get_pal_ver() in src/rvngetpalver.c).

Commit together:
  src/Raven.Pal/pal.ver
  src/Raven.Pal/src/rvngetpalver.c
  src/Sparrow.Server/Platform/Pal.cs
  Directory.Packages.props
  libs/Raven.PAL.7.0.<old>.nupkg (del) + libs/Raven.PAL.7.0.<new>.nupkg (add)
