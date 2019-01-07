// ReSharper disable IdentifierTypo
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Global

using System;

namespace Voron.Platform
{
    public static class PalFlags
    {
        public enum FAIL_CODES : uint
        {
            None = uint.MaxValue,
            SUCCESS = 0,
            FAIL_OPEN_FILE = 1,
            FAIL_SEEK_FILE = 2,
            FAIL_WRITE_FILE = 3,
            FAIL_SYNC_FILE = 4,
            FAIL_NOMEM = 5,
            FAIL_STAT_FILE = 6,
            FAIL_RACE_RETRIES = 7,
            FAIL_PATH_RECURSION = 8,
            FAIL_FLUSH_FILE = 9,
            FAIL_SYSCONF = 10,
            FAIL_PWRITE = 11,
            FAIL_PWRITE_WITH_RETRIES = 12,
            FAIL_MMAP64 = 13,
            FAIL_UNLINK = 14,
            FAIL_CLOSE = 15,
            FAIL_ALLOCATION_NO_RESIZE = 16,
            FAIL_FREE = 17
        };

        [Flags]
        public enum ERRNO_SPECIAL_CODES
        {
            None = 0,
            NO_ERROR = (1 << 0),
            ENOMEM = (1 << 1),
            ENOENT = (1 << 2),
            ENOSPC = (1 << 3)
        }

        public enum MMAP_OPTIONS
        {
            None = 0,
            CopyOnWrite = 1,
        }

        [Flags]
        public enum MSYNC_OPTIONS
        {
            None = 0,
            MS_ASYNC = (1 << 0),
            MS_SYNC = (1 << 1),
            MS_INVALIDATE = (1 << 2)
        }

        [Flags]
        public enum MPROTECT_OPTIONS
        {
            None = 0,
            PROT_READ = (1 << 0),
            PROT_WRITE = (1 << 1),
            PROT_EXEC = (1 << 2),
            PROT_GROWSUP = (1 << 3),
            PROT_GROWSDOWN = (1 << 4)
        }
    }
}
