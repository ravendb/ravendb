using System;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform.Posix
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

    // https://github.com/whotwagner/statx-fun/blob/master/statx.h
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct statx_timestamp
    {
        public Int64 tv_sec;
        public UInt32 tv_nsec;
        Int32 __reserved;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct Statx
    {
        public UInt32 stx_mask;             // Mask of bits indicating filled fields
        public UInt32 stx_blksize;          // Block size for filesystem I/O
        public UInt64 stx_attributes;       // Extra file attribute indicators
        public UInt32 stx_nlink;            // Number of hard links
        public UInt32 stx_uid;              // User ID of owner
        public UInt32 stx_gid;              // Group ID of owner
        public UInt16 stx_mode;             // File type and mode
        private unsafe fixed UInt16 __statx_pad1[1];
        public UInt64 stx_ino;              // Inode number
        public UInt64 stx_size;             // Total size in bytes
        public UInt64 stx_blocks;           // Number of 512B blocks allocated

        public UInt64 stx_attributes_mask;
        /* Mask to show what's supported
           in stx_attributes */

        /* The following fields are file timestamps */
        public statx_timestamp stx_atime;   // Last access
        public statx_timestamp stx_btime;   // Creation
        public statx_timestamp stx_ctime;   // Last status change
        public statx_timestamp stx_mtime;   // Last modification

        /* If this file represents a device, then the next two
           fields contain the ID of the device */
        public UInt32 stx_rdev_major;       // Major ID
        public UInt32 stx_rdev_minor;       // Minor ID

        /* The next two fields contain the ID of the device
           containing the filesystem where the file resides */
        public UInt32 stx_dev_major;        // Major ID
        public UInt32 stx_dev_minor;        // Minor ID
        private unsafe fixed UInt64 __spare2[14];
    }
}
