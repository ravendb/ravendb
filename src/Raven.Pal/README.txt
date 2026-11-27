How to build PAL
Prerequisites:
  - Since we want to keep compatability with older versions of GLIBC, and we require GLIBC 2.3 or lower,
    we must have wsl of ubuntu 20.04 or lower, on x86_64 (linux-x64 is built with the host compiler,
    so make.sh refuses to build it on another architecture, e.g. the WSL of a Windows on ARM machine)
  - Visual Studio with the 'Desktop development with C++' workload and the 'MSVC ARM64 build tools' component.
    'build-all-windows.bat' finds the latest Visual Studio with vswhere and uses the cl that vcvars puts on the PATH,
    so a new Visual Studio or MSVC version needs no change in the scripts.

0. (First time only)
  a. Run './build-all-posix.sh setup' in wsl in order to install the cross compilers and osxcross (to compile for macos)
     ('build-all.ps1' runs it when there is no osxcross folder)
  b. no need to add osxcross bin to PATH - 'build-all-posix.sh' exports it by itself (a manual export
     is only needed when invoking o64-clang / oa64-clang directly, outside of the build scripts)

1. Run .\build-all.ps1
 - it will bump the PAL version - pal.ver file
 - it will update int32_t rvn_get_pal_ver() in src/rvngetpalver.c
 - it builds all 10 binaries (linux x64 / arm / arm64, macos x64 / arm64, windows x86 / x64 / arm64, win7 x86 / x64)
   and copies them to libs/librvnpal only when every one of them was built, for the architecture its name stands for.
 - then it updates 'PAL_VER' in src/Sparrow.Server/Platform/Pal.cs to the same value
   (RavenDB fails to start with IncorrectDllException when the two differ)
 - when a build fails, nothing is copied and PAL_VER is not changed: fix the toolchain and run
   .\build-all.ps1 -keepversion to rebuild at the same version.

2. Commit libs/librvnpal, pal.ver, src/rvngetpalver.c and src/Sparrow.Server/Platform/Pal.cs together.
