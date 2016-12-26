using System;
using System.Runtime.InteropServices;
using System.Security;
using Voron.Platform.Posix;

namespace Sparrow.Platform.Posix
{
    public static unsafe class Syscall
    {
        internal const string LIBC_6 = "libc.so.6";

        [DllImport(LIBC_6, EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = false)]
        [SecurityCritical]
        public static extern IntPtr Copy(byte* dest, byte* src, long count);

        [DllImport(LIBC_6, EntryPoint = "memcmp", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern int Compare(byte* b1, byte* b2, long count);

        [DllImport(LIBC_6, EntryPoint = "memmove", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern int Move(byte* dest, byte* src, long count);

        [DllImport(LIBC_6, EntryPoint = "memset", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern IntPtr Set(byte* dest, int c, long count);

        [DllImport(LIBC_6, EntryPoint = "syscall", SetLastError = true)]
        private static extern long syscall0(long number);

        public static int gettid()
        {
            const int SYS_gettid = 178;
            return (int) syscall0(SYS_gettid);
        }

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int setpriority(int which, int who, int prio);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int getpriority(int which, int who);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int sysinfo(ref sysinfo_t info);

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

        public static unsafe long pread(int fd, void* buf, ulong count, long offset)
        {
            return (long) pread(fd, (IntPtr) buf, (UIntPtr) count, (UIntPtr) offset);
        }

        // posix_fallocate(P)
        //    int posix_fallocate(int fd, off_t offset, size_t len);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int posix_fallocate(int fd, IntPtr offset, UIntPtr len);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int msync(IntPtr start, UIntPtr len, MsyncFlags flags);


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr mmap(IntPtr start, UIntPtr length,
            MmapProts prot, MmapFlags flags, int fd, IntPtr offset);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int munmap(IntPtr start, UIntPtr length);


        // getpid(2)
        //    pid_t getpid(void);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int getpid();

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int unlink(
            [MarshalAs(UnmanagedType.LPStr)] string filename);

        // open(2)
        //    int open(const char *pathname, int flags, mode_t mode);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int open(
            [MarshalAs(UnmanagedType.LPStr)] string pathname,
            [MarshalAs(UnmanagedType.I4)] OpenFlags flags,
            [MarshalAs(UnmanagedType.U2)] FilePermissions mode);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int fsync(int fd);


        // read(2)
        //    ssize_t read(int fd, void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr read(int fd, IntPtr buf, UIntPtr count);

        public static unsafe long read(int fd, void* buf, ulong count)
        {
            return (long) read(fd, (IntPtr) buf, (UIntPtr) count);
        }


        // pwrite(2)
        //    ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr pwrite(int fd, IntPtr buf, UIntPtr count, IntPtr offset);

        public static unsafe long pwrite(int fd, void* buf, ulong count, long offset)
        {
            return (long) pwrite(fd, (IntPtr) buf, (UIntPtr) count, (IntPtr) offset);
        }


        // write(2)
        //    ssize_t write(int fd, const void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr write(int fd, IntPtr buf, UIntPtr count);

        public static unsafe long write(int fd, void* buf, ulong count)
        {
            return (long) write(fd, (IntPtr) buf, (UIntPtr) count);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern long sysconf(SysconfName name, Errno defaultError);

        public static long sysconf(SysconfName name)
        {
            return sysconf(name, (Errno) 0);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int madvise(IntPtr addr, UIntPtr length, MAdvFlags madvFlags);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int ftruncate(int fd, IntPtr size);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int lseek(int fd, long offset, WhenceFlags whence);

        public static unsafe int AllocateUsingLseek(int fd, long size)
        {
            if (size <= 0)
                return 0;
            var orig = lseek(fd, 0, WhenceFlags.SEEK_CUR);
            var offset = lseek(fd, size - 1, WhenceFlags.SEEK_SET);
            if (offset == -1)
                return offset;

            int zero = 0;

            int rc = (int) write(fd, &zero, 1UL);

            orig = lseek(fd, orig, WhenceFlags.SEEK_SET);
            if (rc == -1)
                return rc;

            return orig;
        }

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mprotect(IntPtr start, ulong size, ProtFlag protFlag);

    }
}