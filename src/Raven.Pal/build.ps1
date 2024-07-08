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

$posix_files = `
    "src/posix/fileutils.c",
    "src/posix/geterrorstring.c",
    "src/posix/getsysteminformation.c",
    "src/posix/journal.c",
    "src/posix/mapping.c",
    "src/posix/pager.c",
    "src/posix/sync.c",
    "src/posix/virtualmemory.c",
    "src/posix/writefileheader.c"

mkdir artifacts -ErrorAction Ignore

echo "Building Windows x86"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86-windows -o artifacts/librvnpal.win.x86.dll $shared $win_files 

echo "Building Windows x64"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86_64-windows -o artifacts/librvnpal.win.x64.dll $shared $win_files 

echo "Building Linux x86"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target x86-linux-gnu -o artifacts/librvnpal.linux.x86.so $shared $posix_files "src/posix/linuxonly.c" 

echo "Building Linux x64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target x86-linux-gnu -o artifacts/librvnpal.linux.x64.so $shared $posix_files "src/posix/linuxonly.c" 

echo "Building Linux ARM32 (Rasbperry Pi)"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target arm-linux-gnueabihf -o artifacts/librvnpal.linux.arm.32.so $shared $posix_files "src/posix/linuxonly.c" 

echo "Building Linux ARM64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target aarch64-linux-gnu -o artifacts/librvnpal.linux.arm.64.so $shared $posix_files "src/posix/linuxonly.c" 

echo "Building Linux Mac x64"
zig cc -Wall -O3  -shared -fPIC -Iinc -target x86_64-macos-none -o artifacts/librvnpal.mac.x64.dylib $shared $posix_files "src/posix/maconly.c" 

echo "Building Linux Mac ARM64"
zig cc -Wall -O3  -shared  -fPIC -Iinc -target aarch64-macos-none -o artifacts/librvnpal.mac.arm64.dylib $shared $posix_files "src/posix/maconly.c" 

copy artifacts/*.dll ..\..\libs\librvnpal\
copy artifacts/*.so ..\..\libs\librvnpal\
copy artifacts/*.dylib ..\..\libs\librvnpal\