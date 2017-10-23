using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Win32;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Raven/Server");

        public static long HighLastOneMinute;
        public static long LowLastOneMinute = long.MaxValue;
        public static long HighLastFiveMinutes;
        public static long LowLastFiveMinutes = long.MaxValue;
        public static long HighSinceStartup;
        public static long LowSinceStartup = long.MaxValue;


        private static bool _failedToGetAvailablePhysicalMemory;
        private static readonly MemoryInfoResult FailedResult = new MemoryInfoResult
        {
            AvailableMemory = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
            InstalledMemory = new Size(256, SizeUnit.Megabytes),
            MemoryUsageRecords =
            new MemoryInfoResult.MemoryUsageLowHigh
            {
                High = new MemoryInfoResult.MemoryUsageIntervals
                {
                    LastFiveMinutes = new Size(0, SizeUnit.Bytes),
                    LastOneMinute = new Size(0, SizeUnit.Bytes),
                    SinceStartup = new Size(0, SizeUnit.Bytes)
                },
                Low = new MemoryInfoResult.MemoryUsageIntervals
                {
                    LastFiveMinutes = new Size(0, SizeUnit.Bytes),
                    LastOneMinute = new Size(0, SizeUnit.Bytes),
                    SinceStartup = new Size(0, SizeUnit.Bytes)
                }
            }
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
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern unsafe bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetPhysicallyInstalledSystemMemory(out long totalMemoryInKb);

        /// <summary>
        /// This value is in MB
        /// </summary>
        public static int MemoryLimit { get; set; }

        public static long GetRssMemoryUsage(int processId)
        {
            // currently Process.GetCurrentProcess().WorkingSet64 doesn't give the real RSS number
            // getting it from /proc/self/stat or statm can be also problematic because in some distros the number is in page size, in other pages, and position is not always guarenteed
            // however /proc/self/status gives the real number in humenly format. We extract this here:
            var path = $"/proc/{processId}/status";
            var vmRssString = KernelVirtualFileSystemUtils.ReadLineFromFile(path, "VmRSS");
            if (vmRssString == null)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read VmRSS from {path}");
                return 0;
            }

            var parsedLine = vmRssString.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parsedLine.Length != 3) // format should be: VmRss: <num> kb
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read VmRSS from {path}. Line was {parsedLine}");
                return 0;
            }

            if (parsedLine[0].Contains("VmRSS:") == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to find VmRSS from {path}. Line was {parsedLine}");
                return 0;
            }

            if (long.TryParse(parsedLine[1], out var result) == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to parse VmRSS from {path}. Line was {parsedLine}");
                return 0;
            }

            switch (parsedLine[2].ToLowerInvariant())
            {
                case "kb":
                    result *= 1024L;
                    break;
                case "mb":
                    result *= 1024L * 1024;
                    break;
                case "gb":
                    result *= 1024L * 1024 * 1024;
                    break;
            }

            return result;
        }

        public static (double InstalledMemory, double UsableMemory) GetMemoryInfoInGb()
        {
            var memoryInformation = GetMemoryInfo();
            var installedMemoryInGb = memoryInformation.InstalledMemory.GetDoubleValue(SizeUnit.Gigabytes);
            var usableMemoryInGb = memoryInformation.TotalPhysicalMemory.GetDoubleValue(SizeUnit.Gigabytes);
            return (installedMemoryInGb, usableMemoryInGb);
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
                if (PlatformDetails.RunningOnPosix == false)
                {
                    // windows
                    var memoryStatus = new MemoryStatusEx
                    {
                        dwLength = (uint)sizeof(MemoryStatusEx)
                    };

                    if (GlobalMemoryStatusEx(&memoryStatus) == false)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    if (GetPhysicallyInstalledSystemMemory(out var installedMemoryInKb) == false)
                        installedMemoryInKb = (long)memoryStatus.ullTotalPhys;

                    SetMemoryRecords((long)memoryStatus.ullAvailPhys);

                    return new MemoryInfoResult
                    {
                        AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                        TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                        InstalledMemory = new Size(installedMemoryInKb, SizeUnit.Kilobytes),
                        MemoryUsageRecords = new MemoryInfoResult.MemoryUsageLowHigh
                        {
                            High = new MemoryInfoResult.MemoryUsageIntervals
                            {
                                LastOneMinute = new Size(HighLastOneMinute, SizeUnit.Bytes),
                                LastFiveMinutes = new Size(HighLastFiveMinutes, SizeUnit.Bytes),
                                SinceStartup = new Size(HighSinceStartup, SizeUnit.Bytes)
                            },
                            Low = new MemoryInfoResult.MemoryUsageIntervals
                            {
                                LastOneMinute = new Size(LowLastOneMinute, SizeUnit.Bytes),
                                LastFiveMinutes = new Size(LowLastFiveMinutes, SizeUnit.Bytes),
                                SinceStartup = new Size(LowSinceStartup, SizeUnit.Bytes)
                            }
                        }
                    };
                }

                // read both cgroup and sysinfo memory stats, and use the lowest if applicable
                var cgroupMemoryLimit = KernelVirtualFileSystemUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/memory/memory.limit_in_bytes");
                var cgroupMemoryUsage = KernelVirtualFileSystemUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/memory/memory.usage_in_bytes");

                ulong availableRamInBytes;
                ulong totalPhysicalMemoryInBytes;

                if (PlatformDetails.RunningOnMacOsx == false)
                {
                    // linux
                    var info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    availableRamInBytes = info.AvailableRam;
                    totalPhysicalMemoryInBytes = info.TotalRam;
                }
                else
                {
                    // macOS
                    var mib = new[] {(int)TopLevelIdentifiersMacOs.CTL_HW, (int)CtkHwIdentifiersMacOs.HW_MEMSIZE};
                    ulong physicalMemory = 0;
                    var len = sizeof(ulong);

                    if (Syscall.sysctl(mib, 2, &physicalMemory, &len, null, UIntPtr.Zero) != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to read physical memory info from MacOS, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    totalPhysicalMemoryInBytes = physicalMemory;

                    uint pageSize;
                    var vmStats = new vm_statistics64();

                    var machPort = Syscall.mach_host_self();
                    var count = sizeof(vm_statistics64) / sizeof(uint);

                    if (Syscall.host_page_size(machPort, &pageSize) != 0 ||
                        Syscall.host_statistics64(machPort, (int)FlavorMacOs.HOST_VM_INFO64, &vmStats, &count) != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to get vm_stats from MacOS, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    availableRamInBytes = vmStats.FreePagesCount * (ulong)pageSize;
                }

                Size availableRam, totalPhysicalMemory;
                if (cgroupMemoryLimit < (long)totalPhysicalMemoryInBytes)
                {
                    availableRam = new Size(cgroupMemoryLimit - cgroupMemoryUsage, SizeUnit.Bytes);
                    totalPhysicalMemory = new Size(cgroupMemoryLimit, SizeUnit.Bytes);
                }
                else
                {
                    availableRam = new Size((long)availableRamInBytes, SizeUnit.Bytes);
                    totalPhysicalMemory = new Size((long)totalPhysicalMemoryInBytes, SizeUnit.Bytes);
                }

                SetMemoryRecords((long)availableRamInBytes);

                return new MemoryInfoResult
                {
                    AvailableMemory = availableRam,
                    TotalPhysicalMemory = totalPhysicalMemory,
                    //TODO: http://issues.hibernatingrhinos.com/issue/RavenDB-8468
                    InstalledMemory = totalPhysicalMemory,
                    MemoryUsageRecords = new MemoryInfoResult.MemoryUsageLowHigh
                    {
                        High = new MemoryInfoResult.MemoryUsageIntervals
                        {
                            LastOneMinute = new Size(HighLastOneMinute, SizeUnit.Bytes),
                            LastFiveMinutes = new Size(HighLastFiveMinutes, SizeUnit.Bytes),
                            SinceStartup = new Size(HighSinceStartup, SizeUnit.Bytes)
                        },
                        Low = new MemoryInfoResult.MemoryUsageIntervals
                        {
                            LastOneMinute = new Size(LowLastOneMinute, SizeUnit.Bytes),
                            LastFiveMinutes = new Size(LowLastFiveMinutes, SizeUnit.Bytes),
                            SinceStartup = new Size(LowSinceStartup, SizeUnit.Bytes)
                        }
                    }
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

        private static readonly ConcurrentQueue<Tuple<long, DateTime>> MemByTime = new ConcurrentQueue<Tuple<long, DateTime>>();

        private static void SetMemoryRecords(long availableRamInBytes)
        {
            var now = DateTime.UtcNow;

            if (HighSinceStartup < availableRamInBytes)
                HighSinceStartup = availableRamInBytes;
            if (LowSinceStartup > availableRamInBytes)
                LowSinceStartup = availableRamInBytes;

            MemByTime.Enqueue(new Tuple<long, DateTime>(availableRamInBytes, now));

            while (MemByTime.TryPeek(out var existing) && 
                (now - existing.Item2) > TimeSpan.FromMinutes(5))
            {
                if (MemByTime.TryDequeue(out _) == false)
                    break;
            }

            long highLastOneMinute = 0;
            long lowLastOneMinute = long.MaxValue;
            long highLastFiveMinutes = 0;
            long lowLastFiveMinutes = long.MaxValue;

            foreach (var item in MemByTime)
            {
                if (now - item.Item2 < TimeSpan.FromMinutes(1))
                {
                    if (highLastOneMinute < item.Item1)
                        highLastOneMinute = item.Item1;
                    if (lowLastOneMinute > item.Item1)
                        lowLastOneMinute = item.Item1;
                }
                if (highLastFiveMinutes < item.Item1)
                    highLastFiveMinutes = item.Item1;
                if (lowLastFiveMinutes > item.Item1)
                    lowLastFiveMinutes = item.Item1;
            }

            HighLastOneMinute = highLastOneMinute;
            LowLastOneMinute = lowLastOneMinute;
            HighLastFiveMinutes = highLastFiveMinutes;
            LowLastFiveMinutes = lowLastFiveMinutes;
        }

        public static bool IsSwappingOnHddInsteadOfSsd()
        {
            if (PlatformDetails.RunningOnPosix)
                return CheckPageFileOnHdd.PosixIsSwappingOnHddInsteadOfSsd();
            return CheckPageFileOnHdd.WindowsIsSwappingOnHddInsteadOfSsd();
        }

        public static unsafe bool WillCauseHardPageFault(byte* addr, long length) => PlatformDetails.RunningOnPosix ? PosixMemoryQueryMethods.WillCauseHardPageFault(addr, length) : Win32MemoryQueryMethods.WillCauseHardPageFault(addr, length);
    }

    public struct MemoryInfoResult
    {
        public class MemoryUsageIntervals
        {
            public Size LastOneMinute;
            public Size LastFiveMinutes;
            public Size SinceStartup;
        }
        public class MemoryUsageLowHigh
        {
            public MemoryUsageIntervals High;
            public MemoryUsageIntervals Low;
        }
        public Size TotalPhysicalMemory;
        public Size InstalledMemory;
        public Size AvailableMemory;
        public MemoryUsageLowHigh MemoryUsageRecords;
    }
}
