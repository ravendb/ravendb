using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Win32;
using Sparrow.Utils;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Raven/Server");

        private static readonly ConcurrentQueue<Tuple<long, DateTime>> MemByTime = new ConcurrentQueue<Tuple<long, DateTime>>();
        private static DateTime _memoryRecordsSet;
        private static readonly TimeSpan MemByTimeThrottleTime = TimeSpan.FromMilliseconds(100);

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
            TotalCommittableMemory = new Size(384, SizeUnit.Megabytes),// also include "page file"
            CurrentCommitCharge = new Size(256, SizeUnit.Megabytes),
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

        public static long GetRssMemoryUsage(int processId)
        {
            var path = $"/proc/{processId}/status";
            return GetMemoryUsageByFilter(path, "VmRSS");
        }

        public static long GetAvailableMemoryFromProcMemInfo()
        {
            var path = "/proc/meminfo";
            return GetMemoryUsageByFilter(path, "MemAvailable"); // this is different then sysinfo freeram+buffered (and the closest to the real free memory)
        }
        
        public static long GetFreeMemoryFromProcMemInfo()
        {
            // MemFree is really different then MemAvailable (while free is usually lower then the real free,
            // and available is only estimated free which sometimes higher then the real free memory)
            var path = "/proc/meminfo";
            return GetMemoryUsageByFilter(path, "MemFree");
        }

        public static long GetMemoryUsageByFilter(string path, string filter)
        {
            // currently Process.GetCurrentProcess().WorkingSet64 doesn't give the real RSS number
            // getting it from /proc/self/stat or statm can be also problematic because in some distros the number is in page size, in other pages, and position is not always guarenteed
            // however /proc/self/status gives the real number in humenly format. We extract this here:
            var filterString = KernelVirtualFileSystemUtils.ReadLineFromFile(path, filter);
            if (filterString == null)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read {filter} from {path}");
                return 0;
            }

            var parsedLine = filterString.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parsedLine.Length != 3) // format should be: {filter}: <num> kb
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read {filter} from {path}. Line was {parsedLine}");
                return 0;
            }

            if (parsedLine[0].Contains($"{filter}:") == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to find {filter} from {path}. Line was {parsedLine}");
                return 0;
            }

            if (long.TryParse(parsedLine[1], out var result) == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to parse {filter} from {path}. Line was {parsedLine}");
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

        public static unsafe MemoryInfoResult GetMemoryInfo(bool useFreeInsteadOfAvailable = false)
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
                        TotalCommittableMemory = new Size((long)memoryStatus.ullTotalPageFile, SizeUnit.Bytes),
                        CurrentCommitCharge = new Size((long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile), SizeUnit.Bytes),
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
                    int rc = 0;
                    ulong totalram = 0;
                    if (PlatformDetails.Is32Bits == false)
                    {
                        var info = new sysinfo_t();
                        rc = Syscall.sysinfo(ref info);
                        totalram = info.TotalRam;
                    }
                    else
                    {
                        var info = new sysinfo_t_32bit();
                        rc = Syscall.sysinfo(ref info);
                        totalram = info.TotalRam;
                    }
                    if (rc != 0)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }

                    if (useFreeInsteadOfAvailable)
                        availableRamInBytes = (ulong)GetFreeMemoryFromProcMemInfo();
                    else
                        availableRamInBytes = (ulong)GetAvailableMemoryFromProcMemInfo();
                    totalPhysicalMemoryInBytes = totalram;
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

                    availableRamInBytes = (vmStats.FreePagesCount + vmStats.InactivePagesCount) * (ulong)pageSize;
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
                    // TODO: figure out what this value should be, probably swap + ram
                    TotalCommittableMemory = totalPhysicalMemory,
                    CurrentCommitCharge = availableRam,

                    AvailableMemory = availableRam,
                    TotalPhysicalMemory = totalPhysicalMemory,
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

        public static (long WorkingSet, long TotalUnmanagedAllocations, long ManagedMemory, long MappedTemp) MemoryStats()
        {
            using (var currentProcess = Process.GetCurrentProcess())
            {
                var workingSet =
                    PlatformDetails.RunningOnPosix == false || PlatformDetails.RunningOnMacOsx
                        ? currentProcess.WorkingSet64
                        : GetRssMemoryUsage(currentProcess.Id);

                long totalUnmanagedAllocations = 0;
                foreach (var stats in NativeMemory.ThreadAllocations.Values)
                {
                    if (stats.ThreadInstance.IsAlive)
                        totalUnmanagedAllocations += stats.TotalAllocated;
                }

                // scratch buffers, compression buffers
                var totalMappedTemp = 0L;
                foreach (var mapping in NativeMemory.FileMapping)
                {
                    if (mapping.Key.EndsWith(".buffers", StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    var maxMapped = 0L;
                    foreach (var singleMapping in mapping.Value)
                    {
                        maxMapped = Math.Max(maxMapped, singleMapping.Value);
                    }

                    totalMappedTemp += maxMapped;
                }

                var managedMemory = GC.GetTotalMemory(false);
                return (workingSet, totalUnmanagedAllocations, managedMemory, totalMappedTemp);
            }
        }

        private static void SetMemoryRecords(long availableRamInBytes)
        {
            var now = DateTime.UtcNow;

            if (HighSinceStartup < availableRamInBytes)
                HighSinceStartup = availableRamInBytes;
            if (LowSinceStartup > availableRamInBytes)
                LowSinceStartup = availableRamInBytes;

            while (MemByTime.TryPeek(out var existing) && 
                (now - existing.Item2) > TimeSpan.FromMinutes(5))
            {
                if (MemByTime.TryDequeue(out _) == false)
                    break;
            }

            if (now - _memoryRecordsSet < MemByTimeThrottleTime)
                return;

            _memoryRecordsSet = now;

            MemByTime.Enqueue(new Tuple<long, DateTime>(availableRamInBytes, now));

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

        public static string IsSwappingOnHddInsteadOfSsd()
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

        public Size TotalCommittableMemory;
        public Size CurrentCommitCharge;

        public Size TotalPhysicalMemory;
        public Size InstalledMemory;
        public Size AvailableMemory;
        public MemoryUsageLowHigh MemoryUsageRecords;
    }
}
