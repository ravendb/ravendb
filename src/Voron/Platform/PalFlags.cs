using System;
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Local
// ReSharper disable IdentifierTypo

namespace Voron.Platform
{
    public static class PalFlags
    {
        [Flags]
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
        };

        [Flags]
        public enum ERRNO_SPECIAL_CODES
        {
            None = 1,
            ENOMEM = 2,
            ENOENT = 4
        }
    }
}
