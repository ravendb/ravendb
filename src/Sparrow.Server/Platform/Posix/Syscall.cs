using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Sparrow.Platform.Posix
{
    public static unsafe class Syscall
    {
        private const string LIBC_6 = "libc";

        [DllImport(LIBC_6, EntryPoint = "syscall", SetLastError = true)]
        public static extern long syscall0(long number);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int sched_setaffinity(int pid, IntPtr cpusetsize, ref ulong cpuset);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern long times(ref TimeSample info);
        
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern long times(ref TimeSample_32bit info);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int readlink(
            [MarshalAs(UnmanagedType.LPStr)] string path,
            byte* buffer, int bufferSize);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mkdir(
            [MarshalAs(UnmanagedType.LPStr)] string filename,
            [MarshalAs(UnmanagedType.U2)] ushort mode);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int close(int fd);

        // pread(2)
        //    ssize_t pread(int fd, void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr pread(int fd, IntPtr buf, UIntPtr count, UIntPtr offset);

        public static long pread(int fd, void* buf, ulong count, long offset)
        {
            return (long)pread(fd, (IntPtr)buf, (UIntPtr)count, (UIntPtr)offset);
        }

        // posix_fallocate(P)
        //    int posix_fallocate(int fd, off_t offset, size_t len);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int posix_fallocate64(int fd, long offset, long len);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int msync(IntPtr start, UIntPtr len, MsyncFlags flags);


        [DllImport(LIBC_6, EntryPoint = "mmap64", SetLastError = true)]
        private static extern IntPtr mmap64_posix(IntPtr start, UIntPtr length,
            MmapProts prot, MmapFlags flags, int fd, long offset);

        [DllImport(LIBC_6, EntryPoint = "mmap", SetLastError = true)]
        private static extern IntPtr mmap64_mac(IntPtr start, UIntPtr length,
            MmapProts prot, MmapFlags flags, int fd, long offset);

        public static IntPtr mmap64(IntPtr start, UIntPtr length,
            MmapProts prot, MmapFlags flags, int fd, long offset)
        {
            if (PlatformDetails.RunningOnMacOsx)
                return mmap64_mac(start, length, prot, flags, fd, offset);
            return mmap64_posix(start, length, prot, flags, fd, offset);
        }

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int munmap(IntPtr start, UIntPtr length);

        // posix_memalign(3)
        //     int posix_memalign(void** memptr, size_t alignment, size_t size);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int posix_memalign(byte** pPtr, IntPtr allignment, IntPtr count);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern void free(IntPtr ptr);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int unlink(
            [MarshalAs(UnmanagedType.LPStr)] string filename);

        // int mincore(void *addr, size_t length, unsigned char *vec);
        // The vec argument must point to an array containing at least (length+PAGE_SIZE-1) / PAGE_SIZE bytes
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mincore(void* addr, IntPtr length, char* vec);

        // flock(2)
        //    int flock(int fd, int operation); 
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int flock(
            int fd,
            FLockOperations operation);

        [Flags]
        public enum FLockOperations
        {
            LOCK_SH = 1,
            LOCK_EX = 2,
            LOCK_NB = 4,
            LOCK_UN = 8
        }

        // open(2)
        //    int open(const char *pathname, int flags, mode_t mode);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int open(
            [MarshalAs(UnmanagedType.LPStr)] string pathname,
            [MarshalAs(UnmanagedType.I4)] OpenFlags flags,
            [MarshalAs(UnmanagedType.U2)] FilePermissions mode);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int fcntl(int fd, FcntlCommands cmd, IntPtr args);

        public static int FSync(int fd)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return fcntl(fd, FcntlCommands.F_FULLFSYNC, IntPtr.Zero); // F_FULLFSYNC ignores args
            }
            return fsync(fd);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        private static extern int fsync(int fd);

        [DllImport(LIBC_6, SetLastError = true)]
        private static extern int readlink(string path, byte* buf, UIntPtr bufsiz);

        // read(2)
        //    ssize_t read(int fd, void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr read(int fd, IntPtr buf, UIntPtr count);

        public static long read(int fd, void* buf, ulong count)
        {
            return (long)read(fd, (IntPtr)buf, (UIntPtr)count);
        }


        // pwrite(2)
        //    ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        private static extern IntPtr pwrite64(int fd, IntPtr buf, UIntPtr count, long offset);

        // pwrite(2)
        //    ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        private static extern IntPtr pwrite(int fd, IntPtr buf, UIntPtr count, IntPtr offset);

        public static long pwrite(int fd, void* buf, ulong count, long offset)
        {
            if (PlatformDetails.Is32Bits)
            {
                var rc = (long)pwrite64(fd, (IntPtr)buf, (UIntPtr)count, (long)offset);
                return rc;
            }

            if (PlatformDetails.RunningOnMacOsx)
                return (long)pwrite(fd, (IntPtr)buf, (UIntPtr)count, (IntPtr)offset);

            return (long)pwrite64(fd, (IntPtr)buf, (UIntPtr)count, offset);
        }


        // write(2)
        //    ssize_t write(int fd, const void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr write(int fd, IntPtr buf, ulong count);

        public static long write(int fd, void* buf, ulong count)
        {
            return (long)write(fd, (IntPtr)buf, count);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern long sysconf(int name, Errno defaultError);

        public static long sysconf(int name)
        {
            return sysconf(name, 0);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int madvise(IntPtr addr, UIntPtr length, MAdvFlags madvFlags);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int ftruncate(int fd, IntPtr size);

        [DllImport(LIBC_6, EntryPoint = "lseek64", SetLastError = true)]
        public static extern long lseek64_posix(int fd, long offset, WhenceFlags whence);

        [DllImport(LIBC_6, EntryPoint = "lseek", SetLastError = true)]
        public static extern long lseek64_mac(int fd, long offset, WhenceFlags whence);

        public static long lseek64(int fd, long offset, WhenceFlags whence)
        {
            if (PlatformDetails.RunningOnMacOsx)
                return lseek64_mac(fd, offset, whence);
            return lseek64_posix(fd, offset, whence);
        }

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int statvfs(string path, ref Statvfs buf);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mprotect(IntPtr start, IntPtr size, ProtFlag protFlag);

        public static void PwriteOrThrow(int fd, byte *buffer, ulong count, long offset, string file, string debug)
        {
            Errno err = Errno.NONE;
            bool cifsRetryOccured = false;
            var actuallyWritten = 0L;
            do
            {
                for (var cifsRetries = 0; cifsRetries < 3; cifsRetries++)
                {
                    var result = pwrite(fd, buffer, count - (ulong)actuallyWritten, offset + actuallyWritten);
                    if (result < 0) // we assume zero cannot be returned at any case as defined in POSIX
                    {
                        err = (Errno)Marshal.GetLastWin32Error();
                        if (err == Errno.EINVAL && CheckSyncDirectoryAllowed(file) == false)
                        {
                            // cifs/nfs mount can sometimes fail on EINVAL after file creation
                            // lets give it few retries with short pauses between them - RavenDB-11954
                            Thread.Sleep(10 * ((cifsRetries + 1) ^ 3)); // wait upto 3 time in intervals of : 10mSec, 80mSec, 270mSec
                            cifsRetryOccured = true; // for exception message purpose
                            continue; // retry                            
                        }

                        ThrowLastError((int)err,
                            $"Failed to ${debug} {file} with count={count}, offset={offset}, actuallyWritten={actuallyWritten} (regular mount, not a cifs/nfs mount)");
                    }

                    actuallyWritten += result;
                    break; // successfully written
                }                
            } while (actuallyWritten < (long)count);

            if (actuallyWritten != (long)count)
            {
                ThrowLastError((int)err,
                    $"Failed to pwrite {file} with count={count}, offset={offset}, actuallyWritten={actuallyWritten}" + (cifsRetryOccured ? " (in cifs/nfs mount)" : ""));
            }
        }

        public static int AllocateFileSpace(int fd, long size, string file, out bool usingWrite)
        {
            usingWrite = false;
            int result;
            int retries = 1024;
            while (true)
            {
                var len = new FileInfo(file).Length;
                result = (int)Errno.EINVAL;
                if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) == false)
                    result = posix_fallocate64(fd, len, (size - len));

                switch (result)
                {
                    case (int)Errno.EINVAL:
                    case (int)Errno.EFBIG: // can occure on >4GB allocation on fs such as ntfs-3g, W95 FAT32, etc.
                        // fallocate is not supported, we'll use lseek instead
                        usingWrite = true;
                        byte b = 0;
                        PwriteOrThrow(fd, &b, 1L, size - 1, file, "pwrite in order to fallocate where fallocate is not supported");                        
                        return 0;                        
                }

                if (result != (int)Errno.EINTR)
                    break;
                if (retries-- < 0)
                    throw new IOException($"Tried too many times to call posix_fallocate {file}, but always got EINTR, cannot retry again");
            }
            return result;
        }

        private static long? _pageSize;
        public static long PageSize
        {
            get
            {
                if (_pageSize == null)
                    _pageSize = sysconf(PerPlatformValues.SysconfNames._SC_PAGESIZE);
                return _pageSize.Value;
            }
        }


        public static void ThrowLastError(int lastError, string msg = null)
        {
            if (Enum.IsDefined(typeof(Errno), lastError) == false)
                throw new InvalidOperationException("Unknown errror ='" + lastError + "'. Message: " + msg);
            var error = (Errno)lastError;
            switch (error)
            {
                case Errno.ENOMEM:
                    throw new OutOfMemoryException("ENOMEM on " + msg);
                case Errno.ENOENT:
                    throw new FileNotFoundException("ENOENT on " + msg);
                default:
                    throw new InvalidOperationException(error + " " + msg);
            }
        }

        public static void FsyncDirectoryFor(string file)
        {
            if (CheckSyncDirectoryAllowed(file) && SyncDirectory(file) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                ThrowLastError(err, "FSync dir " + file);
            }
        }

        public static bool CheckSyncDirectoryAllowed(string filepath)
        {
            var allMounts = DriveInfo.GetDrives();
            var syncAllowed = true;
            var matchSize = 0;
            foreach (var m in allMounts)
            {
                var mountNameSize = m.Name.Length;
                if (filepath.StartsWith(m.Name))
                {
                    if (mountNameSize > matchSize)
                    {
                        matchSize = mountNameSize;
                        switch (m.DriveFormat)
                        {
                            case "cifs":
                            case "nfs":
                                syncAllowed = false;
                                break;
                            default:
                                syncAllowed = true;
                                break;
                        }
                        if (m.DriveType == DriveType.Unknown)
                            syncAllowed = false;
                    }
                }
            }
            return syncAllowed;
        }

        private static int ReadLinkOrThrow(string path, byte *pBuff, int buffSize)
        {
            var numOfBytes = readlink(path, pBuff, (UIntPtr)buffSize);
            var err = Marshal.GetLastWin32Error();
            if (numOfBytes == -1)
            {
                if (err == (int)Errno.EINVAL) // EINVAL when applied on non symlink according to readlink's man page 
                    return 0;
                ThrowLastError(err, "failed to readlink symlink directory " + path);
            }

            return numOfBytes;
        }

        public static int SyncDirectory(string file, bool isRealPath = false)
        {
            var dir = isRealPath == false ? Path.GetDirectoryName(file) : file;
            var fd = open(dir, 0, 0);
            if (fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                ThrowLastError(err, "failed to open directory " + dir + " in order to sync for file " + file);
            }
            var fsyncRc = FSync(fd);
            if (fsyncRc == -1)
            {
                var err = Marshal.GetLastWin32Error();
                ThrowLastError(err, "failed to FSync directory " + dir + " with fd=" + fd + " in order to sync for file " + file);
            }

            if (close(fd) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                ThrowLastError(err, "failed to close directory " + dir + " with fd=" + fd + " in order to sync for file " + file);
            }

            if (isRealPath)
                return 0;

            // now sync the real path if the above is a symlink
            var buffSize = 64;
            var realpathToDir = ArrayPool<byte>.Shared.Rent(buffSize);
            try
            {
                // determine buffSize passed to readlink should be using lstat (but this is not available right now)
                // so we try with rented array of 64 bytes and if readlink needs more we will try again with 8K and fail otherwise
                // (TODO: change it to lstat / FileInfo(path).Target in the future when https://github.com/dotnet/corefx/issues/26310 is fixed)
                int numOfBytes;
                fixed (byte* pBuff = realpathToDir)
                {
                    numOfBytes = ReadLinkOrThrow(dir, pBuff, buffSize);
                    if (numOfBytes == 0)
                        return 0; // non symlink dir
 
                    if (numOfBytes >= buffSize) // 64 bytes are not enough for path legnth. lets try with 8192
                    {
                        buffSize = 8192; // usually PATH_MAX is 4096
                        ArrayPool<byte>.Shared.Return(realpathToDir);
                        realpathToDir = ArrayPool<byte>.Shared.Rent(buffSize);
                        
                        fixed (byte* pBuff8K = realpathToDir)
                        {
                            numOfBytes = ReadLinkOrThrow(dir, pBuff8K, buffSize);
                            if (numOfBytes == 0)
                                return 0; // non symlink dir
                            
                            if (numOfBytes >= buffSize)
                            {
                                throw new InvalidOperationException("readlink to " + dir +
                                                                    " failed due to the need of more then 8192 bytes for path legnth. Failed to sync directory");
                            }
                        }
                    }
                }

                var realpathString = Encoding.UTF8.GetString(realpathToDir,0, numOfBytes);
                return SyncDirectory(realpathString, isRealPath: true);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(realpathToDir);
            }
        }

        public static string ErrorNumberToStatusCode(int result)
        {
            return Enum.GetName(typeof(Errno), result);
        }
    }

    [Flags]
    public enum FcntlCommands
    {
        F_NOCACHE = 0x00000030,
        F_FULLFSYNC = 0x00000033
    }

    public unsafe struct Statvfs
    {
        public ulong f_bsize;    /* file system block size */
        public ulong f_frsize;   /* fragment size */
        public ulong f_blocks;   /* size of fs in f_frsize units */
        public ulong f_bfree;    /* # free blocks */
        public ulong f_bavail;   /* # free blocks for unprivileged users */
        public ulong f_files;    /* # inodes */
        public ulong f_ffree;    /* # free inodes */
        public ulong f_favail;   /* # free inodes for unprivileged users */
        public ulong f_fsid;     /* file system ID */
        public ulong f_flag;     /* mount flags */
        public ulong f_namemax;  /* maximum filename length */
        public fixed int f_spare[6];
    }
}
