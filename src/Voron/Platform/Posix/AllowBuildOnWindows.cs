using System;
using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    public static class Syscall
    {
        internal const string LIBC_6 = "libc.so.6";

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int sysinfo(ref sysinfo_t info);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int mkdir (
            [MarshalAs (UnmanagedType.LPStr)] string filename, 
            [MarshalAs (UnmanagedType.U4)] uint mode);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int close(int fd);
        
        // pread(2)
        //    ssize_t pread(int fd, void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr pread(int fd, IntPtr buf, UIntPtr count, UIntPtr offset);

        public static unsafe long pread(int fd, void* buf, ulong count, long offset)
        {
            return (long)pread(fd, (IntPtr)buf, (UIntPtr)count, (UIntPtr)offset);
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
                [MarshalAs(UnmanagedType.LPStr)] string pathname, OpenFlags flags, FilePermissions mode);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int fsync(int fd);


        // read(2)
        //    ssize_t read(int fd, void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr read(int fd, IntPtr buf, UIntPtr count);

        public static unsafe long read(int fd, void* buf, ulong count)
        {
            return (long)read(fd, (IntPtr)buf, (UIntPtr)count);
        }


        // pwrite(2)
        //    ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr pwrite(int fd, IntPtr buf, UIntPtr count, IntPtr offset);

        public static unsafe long pwrite(int fd, void* buf, ulong count, long offset)
        {
            return (long)pwrite(fd, (IntPtr)buf, (UIntPtr)count, (IntPtr)offset);
        }


        // write(2)
        //    ssize_t write(int fd, const void *buf, size_t count);
        [DllImport(LIBC_6, SetLastError = true)]
        public static extern IntPtr write(int fd, IntPtr buf, UIntPtr count);

        public static unsafe long write(int fd, void* buf, ulong count)
        {
            return (long)write(fd, (IntPtr)buf, (UIntPtr)count);
        }


        [DllImport(LIBC_6, SetLastError = true)]
        public static extern long sysconf(SysconfName name, Errno defaultError);

        public static long sysconf(SysconfName name)
        {
            return sysconf(name, (Errno)0);
        }

        [DllImport(LIBC_6, EntryPoint = "__fxstat", SetLastError = true)]
        public static extern int fstat(int version, int filedes, out Stat buf);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int madvise(IntPtr addr, UIntPtr length, MAdvFlags madvFlags);

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int ftruncate(int fd, IntPtr size);
    }

    [Flags]
    public enum MAdvFlags
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

    // Use manually written To/From methods to handle fields st_atime_nsec etc.
    public struct Stat : IEquatable<Stat>
    {
        
        public ulong st_dev;     // device
        
        public ulong st_ino;     // inode
        
        public FilePermissions st_mode;    // protection
#pragma warning disable 169
        private uint _padding_;  // padding for structure alignment
#pragma warning restore 169
        
        public ulong st_nlink;   // number of hard links
        
        public uint st_uid;     // user ID of owner
        
        public uint st_gid;     // group ID of owner
        
        public ulong st_rdev;    // device type (if inode device)
        public long st_size;    // total size, in bytes
        public long st_blksize; // blocksize for filesystem I/O
        public long st_blocks;  // number of blocks allocated
        public long st_atime;   // time of last access
        public long st_mtime;   // time of last modification
        public long st_ctime;   // time of last status change
        public long st_atime_nsec; // Timespec.tv_nsec partner to st_atime
        public long st_mtime_nsec; // Timespec.tv_nsec partner to st_mtime
        public long st_ctime_nsec; // Timespec.tv_nsec partner to st_ctime

        public override int GetHashCode()
        {
            return st_dev.GetHashCode() ^
                st_ino.GetHashCode() ^
                st_mode.GetHashCode() ^
                st_nlink.GetHashCode() ^
                st_uid.GetHashCode() ^
                st_gid.GetHashCode() ^
                st_rdev.GetHashCode() ^
                st_size.GetHashCode() ^
                st_blksize.GetHashCode() ^
                st_blocks.GetHashCode() ^
                st_atime.GetHashCode() ^
                st_mtime.GetHashCode() ^
                st_ctime.GetHashCode() ^
                st_atime_nsec.GetHashCode() ^
                st_mtime_nsec.GetHashCode() ^
                st_ctime_nsec.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
                return false;

            Stat value = (Stat)obj;
            return value.st_dev == st_dev &&
                value.st_ino == st_ino &&
                value.st_mode == st_mode &&
                value.st_nlink == st_nlink &&
                value.st_uid == st_uid &&
                value.st_gid == st_gid &&
                value.st_rdev == st_rdev &&
                value.st_size == st_size &&
                value.st_blksize == st_blksize &&
                value.st_blocks == st_blocks &&
                value.st_atime == st_atime &&
                value.st_mtime == st_mtime &&
                value.st_ctime == st_ctime &&
                value.st_atime_nsec == st_atime_nsec &&
                value.st_mtime_nsec == st_mtime_nsec &&
                value.st_ctime_nsec == st_ctime_nsec;
        }

        public bool Equals(Stat value)
        {
            return value.st_dev == st_dev &&
                value.st_ino == st_ino &&
                value.st_mode == st_mode &&
                value.st_nlink == st_nlink &&
                value.st_uid == st_uid &&
                value.st_gid == st_gid &&
                value.st_rdev == st_rdev &&
                value.st_size == st_size &&
                value.st_blksize == st_blksize &&
                value.st_blocks == st_blocks &&
                value.st_atime == st_atime &&
                value.st_mtime == st_mtime &&
                value.st_ctime == st_ctime &&
                value.st_atime_nsec == st_atime_nsec &&
                value.st_mtime_nsec == st_mtime_nsec &&
                value.st_ctime_nsec == st_ctime_nsec;
        }

        public static bool operator ==(Stat lhs, Stat rhs)
        {
            return lhs.Equals(rhs);
        }

        public static bool operator !=(Stat lhs, Stat rhs)
        {
            return !lhs.Equals(rhs);
        }
    }

    public enum Errno
    {
        // errors & their values liberally copied from
        // FC2 /usr/include/asm/errno.h

        EPERM = 1, // Operation not permitted 
        ENOENT = 2, // No such file or directory 
        ESRCH = 3, // No such process 
        EINTR = 4, // Interrupted system call 
        EIO = 5, // I/O error 
        ENXIO = 6, // No such device or address 
        E2BIG = 7, // Arg list too long 
        ENOEXEC = 8, // Exec format error 
        EBADF = 9, // Bad file number 
        ECHILD = 10, // No child processes 
        EAGAIN = 11, // Try again 
        ENOMEM = 12, // Out of memory 
        EACCES = 13, // Permission denied 
        EFAULT = 14, // Bad address 
        ENOTBLK = 15, // Block device required 
        EBUSY = 16, // Device or resource busy 
        EEXIST = 17, // File exists 
        EXDEV = 18, // Cross-device link 
        ENODEV = 19, // No such device 
        ENOTDIR = 20, // Not a directory 
        EISDIR = 21, // Is a directory 
        EINVAL = 22, // Invalid argument 
        ENFILE = 23, // File table overflow 
        EMFILE = 24, // Too many open files 
        ENOTTY = 25, // Not a typewriter 
        ETXTBSY = 26, // Text file busy 
        EFBIG = 27, // File too large 
        ENOSPC = 28, // No space left on device 
        ESPIPE = 29, // Illegal seek 
        EROFS = 30, // Read-only file system 
        EMLINK = 31, // Too many links 
        EPIPE = 32, // Broken pipe 
        EDOM = 33, // Math argument out of domain of func 
        ERANGE = 34, // Math result not representable 
        EDEADLK = 35, // Resource deadlock would occur 
        ENAMETOOLONG = 36, // File name too long 
        ENOLCK = 37, // No record locks available 
        ENOSYS = 38, // Function not implemented 
        ENOTEMPTY = 39, // Directory not empty 
        ELOOP = 40, // Too many symbolic links encountered 
        EWOULDBLOCK = EAGAIN, // Operation would block 
        ENOMSG = 42, // No message of desired type 
        EIDRM = 43, // Identifier removed 
        ECHRNG = 44, // Channel number out of range 
        EL2NSYNC = 45, // Level 2 not synchronized 
        EL3HLT = 46, // Level 3 halted 
        EL3RST = 47, // Level 3 reset 
        ELNRNG = 48, // Link number out of range 
        EUNATCH = 49, // Protocol driver not attached 
        ENOCSI = 50, // No CSI structure available 
        EL2HLT = 51, // Level 2 halted 
        EBADE = 52, // Invalid exchange 
        EBADR = 53, // Invalid request descriptor 
        EXFULL = 54, // Exchange full 
        ENOANO = 55, // No anode 
        EBADRQC = 56, // Invalid request code 
        EBADSLT = 57, // Invalid slot 

        EDEADLOCK = EDEADLK,

        EBFONT = 59, // Bad font file format 
        ENOSTR = 60, // Device not a stream 
        ENODATA = 61, // No data available 
        ETIME = 62, // Timer expired 
        ENOSR = 63, // Out of streams resources 
        ENONET = 64, // Machine is not on the network 
        ENOPKG = 65, // Package not installed 
        EREMOTE = 66, // Object is remote 
        ENOLINK = 67, // Link has been severed 
        EADV = 68, // Advertise error 
        ESRMNT = 69, // Srmount error 
        ECOMM = 70, // Communication error on send 
        EPROTO = 71, // Protocol error 
        EMULTIHOP = 72, // Multihop attempted 
        EDOTDOT = 73, // RFS specific error 
        EBADMSG = 74, // Not a data message 
        EOVERFLOW = 75, // Value too large for defined data type 
        ENOTUNIQ = 76, // Name not unique on network 
        EBADFD = 77, // File descriptor in bad state 
        EREMCHG = 78, // Remote address changed 
        ELIBACC = 79, // Can not access a needed shared library 
        ELIBBAD = 80, // Accessing a corrupted shared library 
        ELIBSCN = 81, // .lib section in a.out corrupted 
        ELIBMAX = 82, // Attempting to link in too many shared libraries 
        ELIBEXEC = 83, // Cannot exec a shared library directly 
        EILSEQ = 84, // Illegal byte sequence 
        ERESTART = 85, // Interrupted system call should be restarted 
        ESTRPIPE = 86, // Streams pipe error 
        EUSERS = 87, // Too many users 
        ENOTSOCK = 88, // Socket operation on non-socket 
        EDESTADDRREQ = 89, // Destination address required 
        EMSGSIZE = 90, // Message too long 
        EPROTOTYPE = 91, // Protocol wrong type for socket 
        ENOPROTOOPT = 92, // Protocol not available 
        EPROTONOSUPPORT = 93, // Protocol not supported 
        ESOCKTNOSUPPORT = 94, // Socket type not supported 
        EOPNOTSUPP = 95, // Operation not supported on transport endpoint 
        EPFNOSUPPORT = 96, // Protocol family not supported 
        EAFNOSUPPORT = 97, // Address family not supported by protocol 
        EADDRINUSE = 98, // Address already in use 
        EADDRNOTAVAIL = 99, // Cannot assign requested address 
        ENETDOWN = 100, // Network is down 
        ENETUNREACH = 101, // Network is unreachable 
        ENETRESET = 102, // Network dropped connection because of reset 
        ECONNABORTED = 103, // Software caused connection abort 
        ECONNRESET = 104, // Connection reset by peer 
        ENOBUFS = 105, // No buffer space available 
        EISCONN = 106, // Transport endpoint is already connected 
        ENOTCONN = 107, // Transport endpoint is not connected 
        ESHUTDOWN = 108, // Cannot send after transport endpoint shutdown 
        ETOOMANYREFS = 109, // Too many references: cannot splice 
        ETIMEDOUT = 110, // Connection timed out 
        ECONNREFUSED = 111, // Connection refused 
        EHOSTDOWN = 112, // Host is down 
        EHOSTUNREACH = 113, // No route to host 
        EALREADY = 114, // Operation already in progress 
        EINPROGRESS = 115, // Operation now in progress 
        ESTALE = 116, // Stale NFS file handle 
        EUCLEAN = 117, // IStructure needs cleaning 
        ENOTNAM = 118, // Not a XENIX named type file 
        ENAVAIL = 119, // No XENIX semaphores available 
        EISNAM = 120, // Is a named type file 
        EREMOTEIO = 121, // Remote I/O error 
        EDQUOT = 122, // Quota exceeded 

        ENOMEDIUM = 123, // No medium found 
        EMEDIUMTYPE = 124, // Wrong medium type 

        ECANCELED = 125,
        ENOKEY = 126,
        EKEYEXPIRED = 127,
        EKEYREVOKED = 128,
        EKEYREJECTED = 129,

        EOWNERDEAD = 130,
        ENOTRECOVERABLE = 131,

        // OS X-specific values: OS X value + 1000
        EPROCLIM = 1067, // Too many processes
        EBADRPC = 1072, // RPC struct is bad
        ERPCMISMATCH = 1073,    // RPC version wrong
        EPROGUNAVAIL = 1074,    // RPC prog. not avail
        EPROGMISMATCH = 1075,   // Program version wrong
        EPROCUNAVAIL = 1076,    // Bad procedure for program
        EFTYPE = 1079,  // Inappropriate file type or format
        EAUTH = 1080,   // Authentication error
        ENEEDAUTH = 1081,   // Need authenticator
        EPWROFF = 1082, // Device power is off
        EDEVERR = 1083, // Device error, e.g. paper out
        EBADEXEC = 1085,    // Bad executable
        EBADARCH = 1086,    // Bad CPU type in executable
        ESHLIBVERS = 1087,  // Shared library version mismatch
        EBADMACHO = 1088,   // Malformed Macho file
        ENOATTR = 1093, // Attribute not found
        ENOPOLICY = 1103,   // No such policy registered
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
        O_DIRECT = 16384, // 0x00004000, // value directly from printf("%d", O_DIRECT)

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
        O_DIRECTORY = 0x00010000,
        O_ASYNC = 0x00002000,
        O_LARGEFILE = 0x00008000,
        O_CLOEXEC = 0x00080000,
        O_PATH = 0x00200000
    }

    // mode_t
    [Flags]
    
    public enum FilePermissions : uint
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


    public enum SysconfName
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
    public struct sysinfo_t
    {
        public System.UIntPtr  uptime;             /* Seconds since boot */
        [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst=3)]
        public System.UIntPtr [] loads;  /* 1, 5, and 15 minute load averages */
        public System.UIntPtr totalram;  /* Total usable main memory size */

        public System.UIntPtr freeram;   /* Available memory size */
        public ulong AvailableRam {
            get { return (ulong)freeram; }
            set { freeram = new UIntPtr (value); }
        }
        public ulong TotalRam
        {
            get { return (ulong)totalram; }
            set { totalram = new UIntPtr(value); }
        }

        public System.UIntPtr sharedram; /* Amount of shared memory */
        public System.UIntPtr bufferram; /* Memory used by buffers */
        public System.UIntPtr totalswap; /* Total swap space size */
        public System.UIntPtr freeswap;  /* swap space still available */
        public ushort procs;    /* Number of current processes */
        [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst=22)]
        public char[] _f; /* Pads structure to 64 bytes */
    }

}
