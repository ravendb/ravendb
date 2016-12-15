using System;
using System.Runtime.InteropServices;
using Sparrow;

namespace Voron.Platform.Posix
{
    public static class Syscall
    {
        internal const string LIBC_6 = "libc.so.6";

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
    }


    [Flags]
    public enum WhenceFlags : int
    {
        SEEK_SET = 0,
        SEEK_CUR = 1,
        SEEK_END = 2
    }

    [Flags]
    public enum MAdvFlags : int
    {
        MADV_NORMAL = 0x0,  /* No further special treatment */
        MADV_RANDOM = 0x1, /* Expect random page references */
        MADV_SEQUENTIAL = 0x2,  /* Expect sequential page references */
        MADV_WILLNEED = 0x3, /* Will need these pages */
        MADV_DONTNEED = 0x4, /* Don't need these pages */
        MADV_FREE = 0x5, /* Contents can be freed */
        MADV_ACCESS_DEFAULT = 0x6, /* default access */
        MADV_ACCESS_LWP = 0x7, /* next LWP to access heavily */
        MADV_ACCESS_MANY = 0x8, /* many processes to access heavily */
    }

    [Flags]
    public enum MmapProts : int
    {
        PROT_READ = 0x1,  // Page can be read.
        PROT_WRITE = 0x2,  // Page can be written.
        PROT_EXEC = 0x4,  // Page can be executed.
        PROT_NONE = 0x0,  // Page can not be accessed.
        PROT_GROWSDOWN = 0x01000000, // Extend change to start of
                                     //   growsdown vma (mprotect only).
        PROT_GROWSUP = 0x02000000, // Extend change to start of
                                   //   growsup vma (mprotect only).
    }


    public enum MmapFlags : int
    {
        MAP_SHARED = 0x01,     // Share changes.
        MAP_PRIVATE = 0x02,     // Changes are private.
        MAP_TYPE = 0x0f,     // Mask for type of mapping.
        MAP_FIXED = 0x10,     // Interpret addr exactly.
        MAP_FILE = 0,
        MAP_ANONYMOUS = 0x20,     // Don't use a file.
        MAP_ANON = MAP_ANONYMOUS,

        // These are Linux-specific.
        MAP_GROWSDOWN = 0x00100,  // Stack-like segment.
        MAP_DENYWRITE = 0x00800,  // ETXTBSY
        MAP_EXECUTABLE = 0x01000,  // Mark it as an executable.
        MAP_LOCKED = 0x02000,  // Lock the mapping.
        MAP_NORESERVE = 0x04000,  // Don't check for reservations.
        MAP_POPULATE = 0x08000,  // Populate (prefault) pagetables.
        MAP_NONBLOCK = 0x10000,  // Do not block on IO.
    }


    public enum MsyncFlags : int
    {
        MS_ASYNC = 0x1,  // Sync memory asynchronously.
        MS_SYNC = 0x4,  // Synchronous memory sync.
        MS_INVALIDATE = 0x2,  // Invalidate the caches.
    }

    public struct Iovec
    {
        public IntPtr iov_base; // Starting address
        
        public UIntPtr iov_len;  // Number of bytes to transfer
    }



    public class OpenFlagsThatAreDifferentBetweenPlatforms
    {
        public static OpenFlags O_DIRECT =(OpenFlags)(
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 65536 // value directly from printf("%d", O_DIRECT) on the pi
                : 16384); // value directly from printf("%d", O_DIRECT)

        public static OpenFlags O_DIRECTORY = (OpenFlags) (
        (RuntimeInformation.OSArchitecture == Architecture.Arm ||
         RuntimeInformation.OSArchitecture == Architecture.Arm64)
            ? 16384 // value directly from printf("%d", O_DIRECTORY)
            : 65536); // value directly from printf("%d", O_DIRECTORY) on the pi

    }

    [Flags]
    
    public enum OpenFlags : int
    {
        //
        // One of these
        //
        O_RDONLY = 0x00000000,
        O_WRONLY = 0x00000001,
        O_RDWR = 0x00000002,

        //
        // Or-ed with zero or more of these
        //
        O_CREAT = 0x00000040,
        O_EXCL = 0x00000080,
        O_NOCTTY = 0x00000100,
        O_TRUNC = 0x00000200,
        O_APPEND = 0x00000400,
        O_NONBLOCK = 0x00000800,
        O_SYNC = 1052672, // 0x00101000, // value directly from printf("%d", O_SYNC)
        O_DSYNC = 4096, // 0x00001000, // value directly from printf("%d", O_DSYNC)

        //
        // These are non-Posix.  Using them will result in errors/exceptions on
        // non-supported platforms.
        //
        // (For example, "C-wrapped" system calls -- calls with implementation in
        // MonoPosixHelper -- will return -1 with errno=EINVAL.  C#-wrapped system
        // calls will generate an exception in NativeConvert, as the value can't be
        // converted on the target platform.)
        //

        O_NOFOLLOW = 0x00020000,
        O_ASYNC = 0x00002000,
        O_LARGEFILE = 0x00008000,
        O_CLOEXEC = 0x00080000,
        O_PATH = 0x00200000
    }

    // mode_t
    [Flags]
    
    public enum FilePermissions : ushort
    {
        S_ISUID = 0x0800, // Set user ID on execution
        S_ISGID = 0x0400, // Set group ID on execution
        S_ISVTX = 0x0200, // Save swapped text after use (sticky).
        S_IRUSR = 0x0100, // Read by owner
        S_IWUSR = 0x0080, // Write by owner
        S_IXUSR = 0x0040, // Execute by owner
        S_IRGRP = 0x0020, // Read by group
        S_IWGRP = 0x0010, // Write by group
        S_IXGRP = 0x0008, // Execute by group
        S_IROTH = 0x0004, // Read by other
        S_IWOTH = 0x0002, // Write by other
        S_IXOTH = 0x0001, // Execute by other

        S_IRWXG = (S_IRGRP | S_IWGRP | S_IXGRP),
        S_IRWXU = (S_IRUSR | S_IWUSR | S_IXUSR),
        S_IRWXO = (S_IROTH | S_IWOTH | S_IXOTH),
        ACCESSPERMS = (S_IRWXU | S_IRWXG | S_IRWXO), // 0777
        ALLPERMS = (S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO), // 07777
        DEFFILEMODE = (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH), // 0666

        // Device types
        // Why these are held in "mode_t" is beyond me...
        S_IFMT = 0xF000, // Bits which determine file type
        S_IFDIR = 0x4000, // Directory
        S_IFCHR = 0x2000, // Character device
        S_IFBLK = 0x6000, // Block device
        S_IFREG = 0x8000, // Regular file
        S_IFIFO = 0x1000, // FIFO
        S_IFLNK = 0xA000, // Symbolic link
        S_IFSOCK = 0xC000, // Socket
    }


    public enum SysconfName : int
    {
        _SC_ARG_MAX,
        _SC_CHILD_MAX,
        _SC_CLK_TCK,
        _SC_NGROUPS_MAX,
        _SC_OPEN_MAX,
        _SC_STREAM_MAX,
        _SC_TZNAME_MAX,
        _SC_JOB_CONTROL,
        _SC_SAVED_IDS,
        _SC_REALTIME_SIGNALS,
        _SC_PRIORITY_SCHEDULING,
        _SC_TIMERS,
        _SC_ASYNCHRONOUS_IO,
        _SC_PRIORITIZED_IO,
        _SC_SYNCHRONIZED_IO,
        _SC_FSYNC,
        _SC_MAPPED_FILES,
        _SC_MEMLOCK,
        _SC_MEMLOCK_RANGE,
        _SC_MEMORY_PROTECTION,
        _SC_MESSAGE_PASSING,
        _SC_SEMAPHORES,
        _SC_SHARED_MEMORY_OBJECTS,
        _SC_AIO_LISTIO_MAX,
        _SC_AIO_MAX,
        _SC_AIO_PRIO_DELTA_MAX,
        _SC_DELAYTIMER_MAX,
        _SC_MQ_OPEN_MAX,
        _SC_MQ_PRIO_MAX,
        _SC_VERSION,
        _SC_PAGESIZE,
        _SC_RTSIG_MAX,
        _SC_SEM_NSEMS_MAX,
        _SC_SEM_VALUE_MAX,
        _SC_SIGQUEUE_MAX,
        _SC_TIMER_MAX,
        /* Values for the argument to `sysconf'
             corresponding to _POSIX2_* symbols.  */
        _SC_BC_BASE_MAX,
        _SC_BC_DIM_MAX,
        _SC_BC_SCALE_MAX,
        _SC_BC_STRING_MAX,
        _SC_COLL_WEIGHTS_MAX,
        _SC_EQUIV_CLASS_MAX,
        _SC_EXPR_NEST_MAX,
        _SC_LINE_MAX,
        _SC_RE_DUP_MAX,
        _SC_CHARCLASS_NAME_MAX,
        _SC_2_VERSION,
        _SC_2_C_BIND,
        _SC_2_C_DEV,
        _SC_2_FORT_DEV,
        _SC_2_FORT_RUN,
        _SC_2_SW_DEV,
        _SC_2_LOCALEDEF,
        _SC_PII,
        _SC_PII_XTI,
        _SC_PII_SOCKET,
        _SC_PII_INTERNET,
        _SC_PII_OSI,
        _SC_POLL,
        _SC_SELECT,
        _SC_UIO_MAXIOV,
        _SC_IOV_MAX = _SC_UIO_MAXIOV,
        _SC_PII_INTERNET_STREAM,
        _SC_PII_INTERNET_DGRAM,
        _SC_PII_OSI_COTS,
        _SC_PII_OSI_CLTS,
        _SC_PII_OSI_M,
        _SC_T_IOV_MAX,
        /* Values according to POSIX 1003.1c (POSIX threads).  */
        _SC_THREADS,
        _SC_THREAD_SAFE_FUNCTIONS,
        _SC_GETGR_R_SIZE_MAX,
        _SC_GETPW_R_SIZE_MAX,
        _SC_LOGIN_NAME_MAX,
        _SC_TTY_NAME_MAX,
        _SC_THREAD_DESTRUCTOR_ITERATIONS,
        _SC_THREAD_KEYS_MAX,
        _SC_THREAD_STACK_MIN,
        _SC_THREAD_THREADS_MAX,
        _SC_THREAD_ATTR_STACKADDR,
        _SC_THREAD_ATTR_STACKSIZE,
        _SC_THREAD_PRIORITY_SCHEDULING,
        _SC_THREAD_PRIO_INHERIT,
        _SC_THREAD_PRIO_PROTECT,
        _SC_THREAD_PROCESS_SHARED,
        _SC_NPROCESSORS_CONF,
        _SC_NPROCESSORS_ONLN,
        _SC_PHYS_PAGES,
        _SC_AVPHYS_PAGES,
        _SC_ATEXIT_MAX,
        _SC_PASS_MAX,
        _SC_XOPEN_VERSION,
        _SC_XOPEN_XCU_VERSION,
        _SC_XOPEN_UNIX,
        _SC_XOPEN_CRYPT,
        _SC_XOPEN_ENH_I18N,
        _SC_XOPEN_SHM,
        _SC_2_CHAR_TERM,
        _SC_2_C_VERSION,
        _SC_2_UPE,
        _SC_XOPEN_XPG2,
        _SC_XOPEN_XPG3,
        _SC_XOPEN_XPG4,
        _SC_CHAR_BIT,
        _SC_CHAR_MAX,
        _SC_CHAR_MIN,
        _SC_INT_MAX,
        _SC_INT_MIN,
        _SC_LONG_BIT,
        _SC_WORD_BIT,
        _SC_MB_LEN_MAX,
        _SC_NZERO,
        _SC_SSIZE_MAX,
        _SC_SCHAR_MAX,
        _SC_SCHAR_MIN,
        _SC_SHRT_MAX,
        _SC_SHRT_MIN,
        _SC_UCHAR_MAX,
        _SC_UINT_MAX,
        _SC_ULONG_MAX,
        _SC_USHRT_MAX,
        _SC_NL_ARGMAX,
        _SC_NL_LANGMAX,
        _SC_NL_MSGMAX,
        _SC_NL_NMAX,
        _SC_NL_SETMAX,
        _SC_NL_TEXTMAX,
        _SC_XBS5_ILP32_OFF32,
        _SC_XBS5_ILP32_OFFBIG,
        _SC_XBS5_LP64_OFF64,
        _SC_XBS5_LPBIG_OFFBIG,
        _SC_XOPEN_LEGACY,
        _SC_XOPEN_REALTIME,
        _SC_XOPEN_REALTIME_THREADS,
        _SC_ADVISORY_INFO,
        _SC_BARRIERS,
        _SC_BASE,
        _SC_C_LANG_SUPPORT,
        _SC_C_LANG_SUPPORT_R,
        _SC_CLOCK_SELECTION,
        _SC_CPUTIME,
        _SC_THREAD_CPUTIME,
        _SC_DEVICE_IO,
        _SC_DEVICE_SPECIFIC,
        _SC_DEVICE_SPECIFIC_R,
        _SC_FD_MGMT,
        _SC_FIFO,
        _SC_PIPE,
        _SC_FILE_ATTRIBUTES,
        _SC_FILE_LOCKING,
        _SC_FILE_SYSTEM,
        _SC_MONOTONIC_CLOCK,
        _SC_MULTI_PROCESS,
        _SC_SINGLE_PROCESS,
        _SC_NETWORKING,
        _SC_READER_WRITER_LOCKS,
        _SC_SPIN_LOCKS,
        _SC_REGEXP,
        _SC_REGEX_VERSION,
        _SC_SHELL,
        _SC_SIGNALS,
        _SC_SPAWN,
        _SC_SPORADIC_SERVER,
        _SC_THREAD_SPORADIC_SERVER,
        _SC_SYSTEM_DATABASE,
        _SC_SYSTEM_DATABASE_R,
        _SC_TIMEOUTS,
        _SC_TYPED_MEMORY_OBJECTS,
        _SC_USER_GROUPS,
        _SC_USER_GROUPS_R,
        _SC_2_PBS,
        _SC_2_PBS_ACCOUNTING,
        _SC_2_PBS_LOCATE,
        _SC_2_PBS_MESSAGE,
        _SC_2_PBS_TRACK,
        _SC_SYMLOOP_MAX,
        _SC_STREAMS,
        _SC_2_PBS_CHECKPOINT,
        _SC_V6_ILP32_OFF32,
        _SC_V6_ILP32_OFFBIG,
        _SC_V6_LP64_OFF64,
        _SC_V6_LPBIG_OFFBIG,
        _SC_HOST_NAME_MAX,
        _SC_TRACE,
        _SC_TRACE_EVENT_FILTER,
        _SC_TRACE_INHERIT,
        _SC_TRACE_LOG,
        _SC_LEVEL1_ICACHE_SIZE,
        _SC_LEVEL1_ICACHE_ASSOC,
        _SC_LEVEL1_ICACHE_LINESIZE,
        _SC_LEVEL1_DCACHE_SIZE,
        _SC_LEVEL1_DCACHE_ASSOC,
        _SC_LEVEL1_DCACHE_LINESIZE,
        _SC_LEVEL2_CACHE_SIZE,
        _SC_LEVEL2_CACHE_ASSOC,
        _SC_LEVEL2_CACHE_LINESIZE,
        _SC_LEVEL3_CACHE_SIZE,
        _SC_LEVEL3_CACHE_ASSOC,
        _SC_LEVEL3_CACHE_LINESIZE,
        _SC_LEVEL4_CACHE_SIZE,
        _SC_LEVEL4_CACHE_ASSOC,
        _SC_LEVEL4_CACHE_LINESIZE
    }

    [StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    public unsafe struct sysinfo_t
    {
        public long  uptime;             /* Seconds since boot */
        public fixed ulong loads[3];  /* 1, 5, and 15 minute load averages */
        public ulong totalram;  /* Total usable main memory size */
        public ulong freeram;   /* Available memory size */
        public ulong sharedram; /* Amount of shared memory */
        public ulong bufferram; /* Memory used by buffers */
        public ulong totalswap; /* Total swap space size */
        public ulong freeswap;  /* swap space still available */
        public ushort procs;    /* Number of current processes */
        public ulong totalhigh; /* Total high memory size */
        public ulong freehigh;  /* Available high memory size */
        public uint mem_unit; /* Memory unit size in bytes */

        public ulong AvailableRam {
            get { return freeram; }
            set { freeram =  value; }
        }
        public ulong TotalRam
        {
            get { return totalram; }
            set { totalram = value; }
        }
    }

}
