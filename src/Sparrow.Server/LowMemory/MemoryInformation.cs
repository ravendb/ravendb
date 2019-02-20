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

        private static readonly int ProcessId;

        static MemoryInformation()
        {
            using (var process = Process.GetCurrentProcess())
                ProcessId = process.Id;
        }

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

        private static float _minimumFreeCommittedMemoryPercentage = 0.05f;
        private static Size _maxFreeCommittedMemoryToKeep = new Size(128, SizeUnit.Megabytes);
        private static Size _lowMemoryCommitLimitInMb = new Size(512, SizeUnit.Megabytes);

        public static void SetFreeCommittedMemory(float minimumFreeCommittedMemoryPercentage, Size maxFreeCommittedMemoryToKeep, Size lowMemoryCommitLimitInMb)
        {
            if (minimumFreeCommittedMemoryPercentage <= 0)
                throw new ArgumentException($"MinimumFreeCommittedMemory must be positive, but was: {minimumFreeCommittedMemoryPercentage}");

            _minimumFreeCommittedMemoryPercentage = minimumFreeCommittedMemoryPercentage;
            _maxFreeCommittedMemoryToKeep = maxFreeCommittedMemoryToKeep;
            _lowMemoryCommitLimitInMb = lowMemoryCommitLimitInMb;
        }

        public static void AssertNotAboutToRunOutOfMemory()
        {
            if (EnableEarlyOutOfMemoryChecks == false)
                return;

            if (DisableEarlyOutOfMemoryCheck)
                return;

            if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
                EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
                return;

            var memInfo = GetMemoryInfo();
            if (IsEarlyOutOfMemoryInternal(memInfo, earlyOutOfMemoryWarning: false, out _))
                ThrowInsufficientMemory(memInfo);
        }

        public static bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
                EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
            {
                commitChargeThreshold = Size.Zero;
                return false;
            }

            return IsEarlyOutOfMemoryInternal(memInfo, earlyOutOfMemoryWarning: true, out commitChargeThreshold);
        }

        private static bool IsEarlyOutOfMemoryInternal(MemoryInfoResult memInfo, bool earlyOutOfMemoryWarning, out Size commitChargeThreshold)
        {
            // if we are about to create a new thread, might not always be a good idea:
            // https://ayende.com/blog/181537-B/production-test-run-overburdened-and-under-provisioned
            // https://ayende.com/blog/181569-A/threadpool-vs-pool-thread

            Size overage;
            if (memInfo.CurrentCommitCharge > memInfo.TotalCommittableMemory)
            {
                // this can happen on containers, since we get this information from the host, and
                // sometimes this kind of stat is shared, see: 
                // https://fabiokung.com/2014/03/13/memory-inside-linux-containers/

                commitChargeThreshold = GetMinCommittedToKeep(memInfo.TotalPhysicalMemory);
                overage =
                    commitChargeThreshold +                                    //extra to keep free
                    (memInfo.TotalPhysicalMemory - memInfo.AvailableMemory);   //actually in use now

                return overage >= memInfo.TotalPhysicalMemory;
            }

            commitChargeThreshold = GetMinCommittedToKeep(memInfo.TotalCommittableMemory);
            overage = commitChargeThreshold + memInfo.CurrentCommitCharge;
            return overage >= memInfo.TotalCommittableMemory;

            Size GetMinCommittedToKeep(Size currentValue)
            {
                var minFreeToKeep = Size.Min(_maxFreeCommittedMemoryToKeep, currentValue * _minimumFreeCommittedMemoryPercentage);

                if (earlyOutOfMemoryWarning)
                {
                    return Size.Min(
                        _lowMemoryCommitLimitInMb,
                        // needs to be bigger than the MaxFreeCommittedMemoryToKeep
                        Size.Max(currentValue / 20, minFreeToKeep * 1.5));
                }

                return minFreeToKeep;
            }
        }

        private static void ThrowInsufficientMemory(MemoryInfoResult memInfo)
        {
            LowMemoryNotification.Instance.SimulateLowMemoryNotification();
            throw new EarlyOutOfMemoryException($"The amount of available memory to commit on the system is low. Commit charge: {memInfo.CurrentCommitCharge} / {memInfo.TotalCommittableMemory}. Memory: {memInfo.TotalPhysicalMemory - memInfo.AvailableMemory} / {memInfo.TotalPhysicalMemory}");
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern unsafe bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetPhysicallyInstalledSystemMemory(out long totalMemoryInKb);

        private static long GetRssMemoryUsage()
        {
            var path = $"/proc/{ProcessId}/status";

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

        public static MemoryInfoResult GetMemoryInformationUsingOneTimeSmapsReader()
        {
            SmapsReader smapsReader = null;
            byte[][] buffers = null;
            try
            {
                if (PlatformDetails.RunningOnLinux)
                {
                    var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    buffers = new[] { buffer1, buffer2 };
                    smapsReader = new SmapsReader(new[] { buffer1, buffer2 });
                }

                return GetMemoryInfo(smapsReader, extended: true);
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

        internal static (Size MemAvailable, Size TotalMemory, Size Commited, Size CommitLimit, Size AvailableWithoutTotalCleanMemory, Size SharedCleanMemory) GetFromProcMemInfo(SmapsReader smapsReader)
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

        internal static MemoryInfoResult GetMemoryInfo(SmapsReader smapsReader = null, bool extended = false)
        {
            if (_failedToGetAvailablePhysicalMemory)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
                return FailedResult;
            }

            try
            {
                extended &= PlatformDetails.RunningOnLinux == false;

                using (var process = extended ? Process.GetCurrentProcess() : null)
                {
                    if (PlatformDetails.RunningOnPosix == false)
                        return GetMemoryInfoWindows(process);

                    if (PlatformDetails.RunningOnMacOsx)
                        return GetMemoryInfoMacOs(process);

                    return GetMemoryInfoLinux(smapsReader);
                }
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

            var workingSet = new Size(0, SizeUnit.Bytes);
            if (smapsReader != null)
            {
                // extended info is needed
                workingSet.Set(GetRssMemoryUsage(), SizeUnit.Bytes);
            }

            return BuildPosixMemoryInfoResult(
                fromProcMemInfo.MemAvailable,
                fromProcMemInfo.TotalMemory,
                fromProcMemInfo.Commited,
                fromProcMemInfo.CommitLimit,
                fromProcMemInfo.AvailableWithoutTotalCleanMemory,
                fromProcMemInfo.SharedCleanMemory,
                workingSet);
        }

        private static MemoryInfoResult BuildPosixMemoryInfoResult(
            Size availableRam, Size totalPhysicalMemory, Size commitedMemory,
            Size commitLimit, Size availableWithoutTotalCleanMemory,
            Size sharedCleanMemory, Size workingSet)
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
                InstalledMemory = totalPhysicalMemory,
                WorkingSet = workingSet
            };
        }

        private static unsafe MemoryInfoResult GetMemoryInfoMacOs(Process process = null)
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
            var workingSet = new Size(process?.WorkingSet64 ?? 0, SizeUnit.Bytes);

            return BuildPosixMemoryInfoResult(availableRamInBytes, totalPhysicalMemory, commitedMemory, commitLimit, availableWithoutTotalCleanMemory, Size.Zero, workingSet);
        }

        private static unsafe MemoryInfoResult GetMemoryInfoWindows(Process process = null)
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

            var sharedClean = GetSharedCleanInBytes(process);
            SetMemoryRecords((long)memoryStatus.ullAvailPhys);

            return new MemoryInfoResult
            {
                TotalCommittableMemory = new Size((long)memoryStatus.ullTotalPageFile, SizeUnit.Bytes),
                CurrentCommitCharge = new Size((long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile), SizeUnit.Bytes),
                AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                AvailableWithoutTotalCleanMemory = new Size((long)memoryStatus.ullAvailPhys + sharedClean, SizeUnit.Bytes),
                SharedCleanMemory = new Size(sharedClean, SizeUnit.Bytes),
                TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                InstalledMemory = fetchedInstalledMemory ?
                    new Size(installedMemoryInKb, SizeUnit.Kilobytes) :
                    new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                WorkingSet = new Size(process?.WorkingSet64 ?? 0, SizeUnit.Bytes)
            };
        }

        public static long GetSharedCleanInBytes(Process process)
        {
            if (process == null)
                return 0;

            var mappedDirty = 0L;
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var fileMappingInfo = mapping.Value.Value;
                var fileType = fileMappingInfo.FileType;
                if (fileType == NativeMemory.FileType.Data)
                    continue;

                var totalMapped = GetTotalMapped(fileMappingInfo);
                if (fileType == NativeMemory.FileType.ScratchBuffer)
                {
                    // for scratch buffers we have the allocated size
                    var allocated = fileMappingInfo.GetAllocatedSizeFunc?.Invoke() ?? totalMapped;
                    if (allocated < totalMapped / 2)
                    {
                        // using less than half of the size of the scratch buffer
                        mappedDirty += allocated;
                        continue;
                    }
                }

                // we are counting the total mapped size of all the other buffers
                mappedDirty += totalMapped;
            }

            var sharedClean = process.WorkingSet64 - AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes() - AbstractLowMemoryMonitor.GetManagedMemoryInBytes() - mappedDirty;

            // the shared dirty can be larger than the size of the working set
            // this can happen when some of the buffers were paged out
            return Math.Max(0, sharedClean);
        }

        private static long GetTotalMapped(NativeMemory.FileMappingInfo fileMappingInfo)
        {
            var totalMapped = 0L;

            foreach (var singleMapping in fileMappingInfo.Info)
            {
                totalMapped += singleMapping.Value;
            }

            return totalMapped;
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

        public static long GetWorkingSetInBytes()
        {
            if (PlatformDetails.RunningOnLinux)
                return GetRssMemoryUsage();

            using (var currentProcess = Process.GetCurrentProcess())
            {
                return currentProcess.WorkingSet64;
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
    }

    public class EarlyOutOfMemoryException : SystemException
    {
        public EarlyOutOfMemoryException() { }
        public EarlyOutOfMemoryException(string message) : base(message) { }
        public EarlyOutOfMemoryException(string message, Exception inner) : base(message, inner) { }
    }
}
