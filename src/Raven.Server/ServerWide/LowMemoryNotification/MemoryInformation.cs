using System;
using System.ComponentModel;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Bond.Protocols;
using Microsoft.Extensions.Logging;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Extensions;
using Voron;
using Voron.Platform.Posix;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public static class MemoryInformation
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MemoryInformation));

        private static int memoryLimit;
        private static bool memoryLimitSet;
        private static bool failedToGetAvailablePhysicalMemory;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateMemoryResourceNotification(int notificationType);

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        public class MemoryStatusEx
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

            public MemoryStatusEx()
            {
                dwLength = (uint)Marshal.SizeOf(typeof(MemoryStatusEx));
            }
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx lpBuffer);

        /// <summary>
        /// This value is in MB
        /// </summary>
        public static int MemoryLimit
        {
            get { return memoryLimit; }
            set
            {
                memoryLimit = value;
                memoryLimitSet = true;
            }
        }

        private static Size GetMemoryInfo(MemoryInfoType type)
        {
            if (failedToGetAvailablePhysicalMemory)
            {
                Log.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
                return new Size(256, SizeUnit.Megabytes);
            }

            try
            {
                if (StorageEnvironmentOptions.RunningOnPosix)
                {
                    sysinfo_t info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        Log.Warn("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return new Size(256, SizeUnit.Megabytes);
                    }

                    var ram = type == MemoryInfoType.AvailableRam ? info.AvailableRam : info.TotalRam;
                    return new Size((long)ram, SizeUnit.Bytes);
                }

              

                var memoryStatus = new MemoryStatusEx();
                var result = GlobalMemoryStatusEx(memoryStatus);
                if (result == false)
                {
                    Log.Warn("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                    return new Size(256, SizeUnit.Megabytes);
                }

                var winResult = type == MemoryInfoType.AvailableRam ? memoryStatus.ullAvailPhys : memoryStatus.ullTotalPhys;
                return new Size((long)winResult, SizeUnit.Bytes);
            }
            catch (Exception e)
            {
                Log.ErrorException("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                failedToGetAvailablePhysicalMemory = true;

                return new Size(256, SizeUnit.Megabytes);
            }
        }

        public static Size GetTotalPhysicalMemory()
        {
            return GetMemoryInfo(MemoryInfoType.TotalRam);
        }

        public static Size GetAvailableMemory()
        {
            return GetMemoryInfo(MemoryInfoType.AvailableRam);
        }
    }

    public enum MemoryInfoType
    {
        AvailableRam,
        TotalRam,
    }
}