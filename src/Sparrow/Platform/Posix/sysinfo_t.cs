using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    [StructLayout(LayoutKind.Sequential)]
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