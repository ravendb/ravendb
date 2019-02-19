using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct sysinfo_t
    {
        public long uptime;             /* Seconds since boot */
        public fixed ulong loads[3];    /* 1, 5, and 15 minute load averages */
        public ulong totalram;          /* Total usable main memory size */
        public ulong freeram;           /* Available memory size */
        public ulong sharedram;         /* Amount of shared memory */
        public ulong bufferram;         /* Memory used by buffers */
        public ulong totalswap;         /* Total swap space size */
        public ulong freeswap;          /* swap space still available */
        public ushort procs;            /* Number of current processes */
        public ulong totalhigh;         /* Total high memory size */
        public ulong freehigh;          /* Available high memory size */
        public uint mem_unit;           /* Memory unit size in bytes */

        public ulong AvailableRam => freeram;
        public ulong TotalRam => totalram;
        public ulong TotalSwap => totalswap;
    }
    
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct sysinfo_t_32bit
    {
        public int uptime;             /* Seconds since boot */
        public fixed uint loads[3];    /* 1, 5, and 15 minute load averages */
        public uint totalram;          /* Total usable main memory size */
        public uint freeram;           /* Available memory size */
        public uint sharedram;         /* Amount of shared memory */
        public uint bufferram;         /* Memory used by buffers */
        public uint totalswap;         /* Total swap space size */
        public uint freeswap;          /* swap space still available */
        public ushort procs;            /* Number of current processes */
        public uint totalhigh;         /* Total high memory size */
        public uint freehigh;          /* Available high memory size */
        public uint mem_unit;           /* Memory unit size in bytes */
        public fixed char _f[20 - 2 * 4 - 4]; /* Padding: libc5 uses this.. */
        
        public ulong AvailableRam => freeram;
        public ulong TotalRam => totalram;
        public ulong TotalSwap => totalswap;
    }
}
