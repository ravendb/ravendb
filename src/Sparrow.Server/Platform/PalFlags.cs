using System;

namespace Sparrow.Server.Platform
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
            FailPwriteWithRetries = 12,
            FailMmap = 13,
            FailUnlink = 14,
            FailClose = 15,
            FailAllocationNoResize = 16,
            FailFree = 17,
            FailInvalidHandle = 18,
            FailTruncateFile = 19,
            FailGetFileSize = 20,
            FailAllocFile = 21,
            FailReadFile = 22,
            FileSetFilePointer = 23,
            FailSetEndOfFile = 24,
            FailEndOfFile = 25,
            FailPrefetch = 26,
            FailGetFilePath = 27,
            FailTestDurability = 28
        };

        [Flags]
        public enum ErrnoSpecialCodes
        {
            None = 0,
            NoMem = (1 << 0),
            NoEnt = (1 << 1),
            NoSpc = (1 << 2)
        }

        [Flags]
        public enum MmapOptions
        {
            None = 0,
            CopyOnWrite = (1 << 0),
            DeleteOnClose = (1 << 1),
        }

        public enum ProtectRange
        {
            None = 0,
            Protect = 1,
            Unprotect = 2
        }

        [Flags]
        public enum JournalMode
        {
            Safe = 0,
            Danger = 1,
            PureMemory = 2
        }

        public enum DurabilityMode
        {
            None = 0,
            DurabililtySupported = 1,
            DurabilityNotSupported = 2
        }
    }
}
