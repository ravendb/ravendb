using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Posix.macOS;
using Sparrow.Platform.Win32;
using Sparrow.Utils;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Server");

        private static readonly ConcurrentQueue<Tuple<long, DateTime>> MemByTime = new ConcurrentQueue<Tuple<long, DateTime>>();
        private static DateTime _memoryRecordsSet;
        private static readonly TimeSpan MemByTimeThrottleTime = TimeSpan.FromMilliseconds(100);

        private static readonly byte[] VmRss = Encoding.UTF8.GetBytes("VmRSS:");
        private static readonly byte[] MemAvailable = Encoding.UTF8.GetBytes("MemAvailable:");
        private static readonly byte[] MemFree = Encoding.UTF8.GetBytes("MemFree:");
        private static readonly byte[] MemTotal = Encoding.UTF8.GetBytes("MemTotal:");
        private static readonly byte[] SwapTotal = Encoding.UTF8.GetBytes("SwapTotal:");
        private static readonly byte[] Committed_AS = Encoding.UTF8.GetBytes("Committed_AS:");

        private const string CgroupMemoryLimit = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
        private const string CgroupMaxMemoryUsage = "/sys/fs/cgroup/memory/memory.max_usage_in_bytes";
        private const string CgroupMemoryUsage = "/sys/fs/cgroup/memory/memory.usage_in_bytes";

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
            AvailableWithoutTotalCleanMemory = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
            TotalCommittableMemory = new Size(384, SizeUnit.Megabytes),// also include "page file"
            CurrentCommitCharge = new Size(256, SizeUnit.Megabytes),
            InstalledMemory = new Size(256, SizeUnit.Megabytes)
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

        public static bool DisableEarlyOutOfMemoryCheck =
            string.Equals(Environment.GetEnvironmentVariable("RAVEN_DISABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

        public static bool EnableEarlyOutOfMemoryCheck =
           string.Equals(Environment.GetEnvironmentVariable("RAVEN_ENABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

        public static bool EnableEarlyOutOfMemoryChecks = false; // we don't want this to run on the clients

        public static void AssertNotAboutToRunOutOfMemory(float minimumFreeCommittedMemory)
        {
            if (EnableEarlyOutOfMemoryChecks == false)
                return;

            if (DisableEarlyOutOfMemoryCheck)
                return;

            if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
                EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
                return;

            // if we are about to create a new thread, might not always be a good idea:
            // https://ayende.com/blog/181537-B/production-test-run-overburdened-and-under-provisioned
            // https://ayende.com/blog/181569-A/threadpool-vs-pool-thread

            var memInfo = GetMemoryInfo();
            Size overage;
            if (memInfo.CurrentCommitCharge > memInfo.TotalCommittableMemory)
            {
                // this can happen on containers, since we get this information from the host, and
                // sometimes this kind of stat is shared, see: 
                // https://fabiokung.com/2014/03/13/memory-inside-linux-containers/

                overage =
                    (memInfo.TotalPhysicalMemory * minimumFreeCommittedMemory) +  //extra to keep free
                    (memInfo.TotalPhysicalMemory - memInfo.AvailableMemory);      //actually in use now
                if (overage >= memInfo.TotalPhysicalMemory)
                {
                    ThrowInsufficentMemory(memInfo);
                    return;
                }

                return;
            }

            overage = (memInfo.TotalCommittableMemory * minimumFreeCommittedMemory) + memInfo.CurrentCommitCharge;
            if (overage >= memInfo.TotalCommittableMemory)
            {
                ThrowInsufficentMemory(memInfo);
            }
        }

        private static void ThrowInsufficentMemory(MemoryInfoResult memInfo)
        {
            throw new EarlyOutOfMemoryException($"The amount of available memory to commit on the system is low. Commit charge: {memInfo.CurrentCommitCharge} / {memInfo.TotalCommittableMemory}. Memory: {memInfo.TotalPhysicalMemory - memInfo.AvailableMemory} / {memInfo.TotalPhysicalMemory}");
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

            try
            {
                using (var bufferedReader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(path))
                {
                    bufferedReader.ReadFileIntoBuffer();
                    var vmrss = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(VmRss);
                    return vmrss * 1024;// value is in KB, we need to return bytes
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read value from {path}", ex);
                return -1;
            }
        }

        public static MemoryInfoResult GetMemInfoUsingOneTimeSmapsReader()
        {
            SmapsReader smapsReader = null;
            byte[][] buffers = null;
            try
            {
                if (PlatformDetails.RunningOnLinux)
                {
                    var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    buffers = new[] {buffer1, buffer2};
                    smapsReader = new SmapsReader(new[] {buffer1, buffer2});
                }
                return GetMemoryInfo(smapsReader);
            }
            finally
            {
                if (buffers != null)
                {
                    ArrayPool<byte>.Shared.Return(buffers[0]);
                    ArrayPool<byte>.Shared.Return(buffers[1]);
                }
            }
        }

        public static (Size MemAvailable, Size TotalMemory, Size Commited, Size CommitLimit, Size AvailableWithoutTotalCleanMemory, Size SharedCleanMemory) GetFromProcMemInfo(SmapsReader smapsReader)
        {
            const string path = "/proc/meminfo";

            // this is different then sysinfo freeram+buffered (and the closest to the real free memory)
            // MemFree is really different then MemAvailable (while free is usually lower then the real free,
            // and available is only estimated free which sometimes higher then the real free memory)
            // for some distros we have only MemFree
            try
            {
                using (var bufferedReader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(path))
                {
                    bufferedReader.ReadFileIntoBuffer();
                    var memAvailableInKb = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(MemAvailable);
                    var memFreeInKb = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(MemFree);
                    var totalMemInKb = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(MemTotal);
                    var swapTotalInKb = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(SwapTotal);
                    var commitedInKb = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(Committed_AS);

                    var totalClean = new Size(memAvailableInKb, SizeUnit.Kilobytes);
                    var sharedCleanMemory = new Size(0, SizeUnit.Bytes);
                    if (smapsReader != null)
                    {
                        var result = smapsReader.CalculateMemUsageFromSmaps<SmapsReaderNoAllocResults>();
                        totalClean.Add(result.SharedClean, SizeUnit.Bytes);
                        totalClean.Add(result.PrivateClean, SizeUnit.Bytes);
                        sharedCleanMemory.Set(result.SharedClean, SizeUnit.Bytes);
                    }

                    return (
                        MemAvailable: new Size(Math.Max(memAvailableInKb, memFreeInKb), SizeUnit.Kilobytes),
                        TotalMemory: new Size(totalMemInKb, SizeUnit.Kilobytes),
                        Commited: new Size(commitedInKb, SizeUnit.Kilobytes),

                        // on Linux, we use the swap + ram as the commit limit, because the actual limit
                        // is dependent on many different factors
                        CommitLimit: new Size(totalMemInKb + swapTotalInKb, SizeUnit.Kilobytes),
                        AvailableWithoutTotalCleanMemory: totalClean,
                        SharedCleanMemory: sharedCleanMemory
                    );
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read value from {path}", ex);

                return (new Size(), new Size(), new Size(), new Size(), new Size(), new Size());
            }
        }

        public static (double InstalledMemory, double UsableMemory) GetMemoryInfoInGb()
        {
            var memoryInformation = GetMemoryInfo();
            var installedMemoryInGb = memoryInformation.InstalledMemory.GetDoubleValue(SizeUnit.Gigabytes);
            var usableMemoryInGb = memoryInformation.TotalPhysicalMemory.GetDoubleValue(SizeUnit.Gigabytes);
            return (installedMemoryInGb, usableMemoryInGb);
        }

        public static MemoryInfoResult GetMemoryInfo(SmapsReader smapsReader = null)
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
                    return GetMemoryInfoWindows();

                if (PlatformDetails.RunningOnMacOsx)
                    return GetMemoryInfoMacOs();

                return GetMemoryInfoLinux(smapsReader);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                _failedToGetAvailablePhysicalMemory = true;
                return FailedResult;
            }
        }

        private static MemoryInfoResult GetMemoryInfoLinux(SmapsReader smapsReader)
        {
            var fromProcMemInfo = GetFromProcMemInfo(smapsReader);
            var totalPhysicalMemoryInBytes = fromProcMemInfo.TotalMemory.GetValue(SizeUnit.Bytes);

            var cgroupMemoryLimit = KernelVirtualFileSystemUtils.ReadNumberFromCgroupFile(CgroupMemoryLimit);
            var cgroupMaxMemoryUsage = KernelVirtualFileSystemUtils.ReadNumberFromCgroupFile(CgroupMaxMemoryUsage);
            // here we need to deal with _soft_ limit, so we'll take the largest of these values
            var maxMemoryUsage = Math.Max(cgroupMemoryLimit ?? 0, cgroupMaxMemoryUsage ?? 0);
            if (maxMemoryUsage != 0 && maxMemoryUsage <= totalPhysicalMemoryInBytes)
            {
                // running in a limited cgroup
                var commitedMemoryInBytes = 0L;
                var cgroupMemoryUsage = KernelVirtualFileSystemUtils.ReadNumberFromCgroupFile(CgroupMemoryUsage);
                if (cgroupMemoryUsage != null)
                {
                    commitedMemoryInBytes = cgroupMemoryUsage.Value;
                    fromProcMemInfo.Commited.Set(commitedMemoryInBytes, SizeUnit.Bytes);
                    fromProcMemInfo.MemAvailable.Set(maxMemoryUsage - cgroupMemoryUsage.Value, SizeUnit.Bytes);
                }

                fromProcMemInfo.TotalMemory.Set(maxMemoryUsage, SizeUnit.Bytes);
                fromProcMemInfo.CommitLimit.Set(Math.Max(maxMemoryUsage, commitedMemoryInBytes), SizeUnit.Bytes);
            }

            return BuildPosixMemoryInfoResult(
                fromProcMemInfo.MemAvailable,
                fromProcMemInfo.TotalMemory,
                fromProcMemInfo.Commited,
                fromProcMemInfo.CommitLimit,
                fromProcMemInfo.AvailableWithoutTotalCleanMemory,
                fromProcMemInfo.SharedCleanMemory
                );
        }

        private static MemoryInfoResult BuildPosixMemoryInfoResult(Size availableRam, Size totalPhysicalMemory, Size commitedMemory, Size commitLimit, Size availableWithoutTotalCleanMemory, Size sharedCleanMemory)
        {
            SetMemoryRecords(availableRam.GetValue(SizeUnit.Bytes));

            return new MemoryInfoResult
            {
                TotalCommittableMemory = commitLimit,
                CurrentCommitCharge = commitedMemory,

                AvailableMemory = availableRam,
                AvailableWithoutTotalCleanMemory = availableWithoutTotalCleanMemory,
                SharedCleanMemory = sharedCleanMemory,
                TotalPhysicalMemory = totalPhysicalMemory,
                InstalledMemory = totalPhysicalMemory
            };
        }

        private static unsafe MemoryInfoResult GetMemoryInfoMacOs()
        {
            var mib = new[] { (int)TopLevelIdentifiers.CTL_HW, (int)CtkHwIdentifiers.HW_MEMSIZE };
            ulong physicalMemory = 0;
            var len = sizeof(ulong);

            if (macSyscall.sysctl(mib, 2, &physicalMemory, &len, null, UIntPtr.Zero) != 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to read physical memory info from MacOS, error code was: " + Marshal.GetLastWin32Error());
                return FailedResult;
            }

            uint pageSize;
            var vmStats = new vm_statistics64();

            var machPort = macSyscall.mach_host_self();
            var count = sizeof(vm_statistics64) / sizeof(uint);

            if (macSyscall.host_page_size(machPort, &pageSize) != 0 ||
                macSyscall.host_statistics64(machPort, (int)Flavor.HOST_VM_INFO64, &vmStats, &count) != 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to get vm_stats from MacOS, error code was: " + Marshal.GetLastWin32Error());
                return FailedResult;
            }

            // swap usage
            var swapu = new xsw_usage();
            len = sizeof(xsw_usage);
            mib = new[] { (int)TopLevelIdentifiers.CTL_VM, (int)CtlVmIdentifiers.VM_SWAPUSAGE };
            if (macSyscall.sysctl(mib, 2, &swapu, &len, null, UIntPtr.Zero) != 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to read swap info from MacOS, error code was: " + Marshal.GetLastWin32Error());
                return FailedResult;
            }

            var totalPhysicalMemory = new Size((long)physicalMemory, SizeUnit.Bytes);

            /* Free memory: This is RAM that's not being used.
             * Wired memory: Information in this memory can't be moved to the hard disk, so it must stay in RAM. The amount of Wired memory depends on the applications you are using.
             * Active memory: This information is currently in memory, and has been recently used.
             * Inactive memory: This information in memory is not actively being used, but was recently used. */
            var availableRamInBytes = new Size((vmStats.FreePagesCount + vmStats.InactivePagesCount) * pageSize, SizeUnit.Bytes);

            // there is no commited memory value in OSX,
            // this is an approximation: wired + active + swap used
            var commitedMemoryInBytes = (vmStats.WirePagesCount + vmStats.ActivePagesCount) * pageSize + (long)swapu.xsu_used;
            var commitedMemory = new Size(commitedMemoryInBytes, SizeUnit.Bytes);

            // commit limit: physical memory + swap
            var commitLimit = new Size((long)(physicalMemory + swapu.xsu_total), SizeUnit.Bytes);

            var availableWithoutTotalCleanMemory = availableRamInBytes; // mac (unlike other linux distros) does calculate accurate available memory

            return BuildPosixMemoryInfoResult(availableRamInBytes, totalPhysicalMemory, commitedMemory, commitLimit, availableWithoutTotalCleanMemory, Size.Zero);
        }

        private static unsafe MemoryInfoResult GetMemoryInfoWindows()
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

            // The amount of physical memory retrieved by the GetPhysicallyInstalledSystemMemory function 
            // must be equal to or greater than the amount reported by the GlobalMemoryStatusEx function
            // if it is less, the SMBIOS data is malformed and the function fails with ERROR_INVALID_DATA. 
            // Malformed SMBIOS data may indicate a problem with the user's computer.
            var fetchedInstalledMemory = GetPhysicallyInstalledSystemMemory(out var installedMemoryInKb);

            SetMemoryRecords((long)memoryStatus.ullAvailPhys);

            return new MemoryInfoResult
            {
                TotalCommittableMemory = new Size((long)memoryStatus.ullTotalPageFile, SizeUnit.Bytes),
                CurrentCommitCharge = new Size((long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile), SizeUnit.Bytes),
                AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                AvailableWithoutTotalCleanMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                SharedCleanMemory = Size.Zero,
                TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                InstalledMemory = fetchedInstalledMemory ?
                    new Size(installedMemoryInKb, SizeUnit.Kilobytes) :
                    new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
            };
        }

        public static MemoryInfoResult.MemoryUsageLowHigh GetMemoryUsageRecords()
        {
            return new MemoryInfoResult.MemoryUsageLowHigh
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
            };
        }

        public static (long WorkingSet, long TotalUnmanagedAllocations, long ManagedMemory, long MappedTemp) MemoryStats()
        {
            using (var currentProcess = Process.GetCurrentProcess())
            {
                var workingSet = PlatformDetails.RunningOnLinux == false
                        ? currentProcess.WorkingSet64
                        : GetRssMemoryUsage(currentProcess.Id);

                long totalUnmanagedAllocations = 0;
                foreach (var stats in NativeMemory.AllThreadStats)
                {
                    if (stats.IsThreadAlive())
                        totalUnmanagedAllocations += stats.TotalAllocated;
                }

                // scratch buffers, compression buffers
                var totalMappedTemp = 0L;
                foreach (var mapping in NativeMemory.FileMapping)
                {
                    var fileType = mapping.Value.Value.FileType;
                    if (fileType == NativeMemory.FileType.Data)
                    {
                        // we care only about the temp buffers
                        continue;
                    }

                    var maxMapped = 0L;
                    foreach (var singleMapping in mapping.Value.Value.Info)
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

        public static unsafe bool WillCauseHardPageFault(byte* address, long length) => PlatformDetails.RunningOnPosix ? PosixMemoryQueryMethods.WillCauseHardPageFault(address, length) : Win32MemoryQueryMethods.WillCauseHardPageFault(address, length);
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
        public Size AvailableWithoutTotalCleanMemory;
        public Size SharedCleanMemory;
    }


    public class EarlyOutOfMemoryException : Exception
    {
        public EarlyOutOfMemoryException() { }
        public EarlyOutOfMemoryException(string message) : base(message) { }
        public EarlyOutOfMemoryException(string message, Exception inner) : base(message, inner) { }
    }
}
