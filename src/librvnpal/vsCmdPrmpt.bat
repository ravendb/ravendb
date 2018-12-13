# in VS Command Prompt:

# win x86:
"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars32.bat"
cl -Felibrvnpal.x86.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\win\getcurrentthreadid.c src\all\basicwrappers.c /link

# win x64:
"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars64.bat"
cl -Felibrvnpal.x64.dll -I inc /O2 /sdl /experimental:external /external:anglebrackets /external:W0 /Wall /LD src\win\getcurrentthreadid.c src\all\basicwrappers.c /link
