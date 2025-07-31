How to build PAL
Prerequisites:
  - Since we want to keep compatability with older versions of GLIBC, and we require GLIBC 2.3 or lower,
    we must have wsl of ubuntu 20.04 or lower

0. (Assuming that VS version got updated and we have new version of MSVC)
  a. Retarget Raven.Pal project in VS
  b. Go to 'build-all-windows.bat' file and update the following variables: 
    - 'vcbin' to point to the location where vcvars32.bat and vcvars64.bat are located
    - 'clbin' to point to the location where cl.exe is located
  c. Run './build-all-posix.sh setup' for the first time in order to install osxcross tool to compile c files for macos
  d. follow the instructions of './build-all-posix.sh setup' and add the path of the osxcross bin to PATH.
    
1. Run .\build-all.ps1
 - it will bump the PAL version - pal.ver file
 - it will update int32_t rvn_get_pal_ver() 
 
2. Go to Pal.cs file and bump PAL_VER