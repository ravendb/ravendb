using System;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Raven/Server");

        private static int _memoryLimit;
        private static bool _failedToGetAvailablePhysicalMemory;
        private static readonly MemoryInfoResult FailedResult = new MemoryInfoResult
        {
            AvailableMemory = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
        };

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
        [DllImport("kernel32.dll",SetLastError = true)]
        public static extern unsafe bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        /// <summary>
        /// This value is in MB
        /// </summary>
        public static int MemoryLimit
        {
            get { return _memoryLimit; }
            set
            {
                _memoryLimit = value;
            }
        }

        public static unsafe MemoryInfoResult GetMemoryInfo()
        {
            if (_failedToGetAvailablePhysicalMemory)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
                return FailedResult;
            }

            try
            {
                if (PlatformDetails.RunningOnPosix)
                {
                    // read both cgroup and sysinfo memory stats, and use the lowest if applicable
                    long cgroupMemoryLimit = CgroupUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/memory/memory.limit_in_bytes");
                    long cgroupMemoryUsage = CgroupUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/memory/memory.usage_in_bytes");

                    sysinfo_t info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    Size availableRam, totalPhysicalMemory;
                    if (cgroupMemoryLimit < (long)info.TotalRam)
                    {
                        availableRam = new Size(cgroupMemoryLimit - cgroupMemoryUsage, SizeUnit.Bytes);
                        totalPhysicalMemory = new Size(cgroupMemoryLimit, SizeUnit.Bytes);
                    }
                    else
                    {
                        availableRam = new Size((long)info.AvailableRam, SizeUnit.Bytes);
                        totalPhysicalMemory = new Size((long)info.TotalRam, SizeUnit.Bytes);
                    }

                    return new MemoryInfoResult
                    {
                        AvailableMemory = availableRam,
                        TotalPhysicalMemory = totalPhysicalMemory
                    };
                }



                var memoryStatus = new MemoryStatusEx
                {
                    dwLength = (uint)sizeof(MemoryStatusEx)
                };
                var result = GlobalMemoryStatusEx(&memoryStatus);
                if (result == false)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                    return FailedResult;
                }

                return new MemoryInfoResult
                {
                    AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                    TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                };
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                _failedToGetAvailablePhysicalMemory = true;
                return FailedResult;
            }
        }
    }

    public struct MemoryInfoResult
    {
        public Size TotalPhysicalMemory;
        public Size AvailableMemory;
    }
}