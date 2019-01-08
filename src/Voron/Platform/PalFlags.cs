using System;
// ReSharper disable UnusedMember.Global

namespace Voron.Platform
{
    public static class PalFlags
    {
        public enum FailCodes
        {
            None = int.MaxValue,
            Success = 0,
            FailOpenFile = 1,
            FailSeekFile = 2,
            FailWriteFile = 3,
            FailSyncFile = 4,
            FailNoMem = 5,
            FailStatFile = 6,
            FailRaceRetries = 7,
            FailPathRecursion = 8,
            FailFlushFile = 9,
            FailSysconf = 10,
            FailPwrite = 11,
            FailPwriteWithRetries= 12,
            FailMmap = 13,
            FailUnlink = 14,
            FailClose = 15,
            FailAllocationNoResize = 16,
            FailFree = 17,
            FailInvalidHandle = 18
        };

        [Flags]
        public enum ErrnoSpecialCodes
        {
            None = 0,
            NoMem = (1 << 0),
            NoEnt = (1 << 1),
            NoSpc = (1 << 2)
        }

        public enum MmapOptions
        {
            None = 0,
            CopyOnWrite = 1,
        }
    }
}
