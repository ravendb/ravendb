using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;

namespace Raven.Client.Helpers
{
    internal static class EnvironmentHelper
    {
        public static bool Is64BitProcess
        {
            get
            {
                return IntPtr.Size == 8;
            }
        }
        [StructLayout(LayoutKind.Sequential)]
        public struct MemoryStatusEx
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
        }
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static unsafe extern bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        internal const string LIBC_6 = "libc.so.6";

        [DllImport(LIBC_6, SetLastError = true)]
        public static extern int sysinfo(ref sysinfo_t info);

        [StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
        public struct sysinfo_t
        {
            public System.UIntPtr uptime;             /* Seconds since boot */
            [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst = 3)]
            public System.UIntPtr[] loads;  /* 1, 5, and 15 minute load averages */
            public System.UIntPtr totalram;  /* Total usable main memory size */

            public System.UIntPtr freeram;   /* Available memory size */
            public ulong AvailableRam
            {
                get { return (ulong)freeram; }
                set { freeram = new UIntPtr(value); }
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
            [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst = 22)]
            public char[] _f; /* Pads structure to 64 bytes */
        }


        public static bool RunningOnPosix
            => RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
               RuntimeInformation.IsOSPlatform(OSPlatform.OSX);


        public static unsafe ulong AvailablePhysicalMemory
        {
            get
            {
                //TODO: fix this so it wouldn't be duplicating the code 
                if (RunningOnPosix)
                {
                    sysinfo_t info = new sysinfo_t();
                    if (sysinfo(ref info) != 0)
                        throw new Win32Exception();

                    return info.AvailableRam;
                }

                var stats = new MemoryStatusEx
                {
                    dwLength = (uint)sizeof(MemoryStatusEx)
                };
                if (GlobalMemoryStatusEx(&stats) == false)
                    throw new Win32Exception();
                return stats.ullAvailPhys;
            }
        }

        public static string MachineName
        {
            get
            {
                return Environment.MachineName;
            }
        }
    }
}