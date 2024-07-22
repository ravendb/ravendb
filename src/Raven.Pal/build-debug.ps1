

$shared = `
    "src/fileutils_all.c",
"src/rvngetpalver.c"


$win_files = `
    "src/win/fileutils.c",
"src/win/getcurrentthreadid.c",
"src/win/geterrorstring.c",
"src/win/getsysteminformation.c",
"src/win/journal.c",
"src/win/mapping.c",
"src/win/pager.c",
"src/win/virtualmemory.c",
"src/win/writefileheader.c"

zig cc -Wall -O3 -g -shared -fPIC -Iinc -target x86_64-windows -o runtimes/win-x64/native/librvnpal.dll  $shared $win_files 
