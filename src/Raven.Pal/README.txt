How to build PAL

0. (Assuming that VS version got updated and we have new version of MSVC)
  a. Retarget Raven.Pal project in VS
  b. Go to 'build-all-windows.bat' file and update the following variables: 
    - 'vcbin' to point to the location where vcvars32.bat and vcvars64.bat are located
    - 'clbin' to point to the location where cl.exe is located
    
1. Run .\build-all.ps1
 - it will bump the PAL version - pal.ver file
 - it will update int32_t rvn_get_pal_ver() 
 
2. Go to Pal.cs file and bump PAL_VER