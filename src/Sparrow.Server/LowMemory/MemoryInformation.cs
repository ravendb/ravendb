using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Posix.macOS;
using Sparrow.Server.Platform.Win32;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Sparrow.Server.LowMemory
{
    public static partial class MemoryInformation
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(MemoryInformation));

        private static readonly byte[] VmRss = Encoding.UTF8.GetBytes("VmRSS:");
        private static readonly byte[] VmSwap = Encoding.UTF8.GetBytes("VmSwap:");
        private static readonly byte[] MemAvailable = Encoding.UTF8.GetBytes("MemAvailable:");
        private static readonly byte[] MemFree = Encoding.UTF8.GetBytes("MemFree:");
        private static readonly byte[] MemTotal = Encoding.UTF8.GetBytes("MemTotal:");
        private static readonly byte[] SwapTotal = Encoding.UTF8.GetBytes("SwapTotal:");
        private static readonly byte[] Committed_AS = Encoding.UTF8.GetBytes("Committed_AS:");

        private static readonly int ProcessId;
        private static readonly bool WindowsSupportsMemoryCountersEx2;
        private static readonly Size? WindowsInstalledMemory;
        private static readonly IntPtr ProcessHandle = IntPtr.Zero;
        public static readonly Size TotalPhysicalMemory;

        static MemoryInformation()
        {
            using (var process = Process.GetCurrentProcess())
            {
                ProcessId = process.Id;

            if (PlatformDetails.RunningOnWindows)
                {
                    ProcessHandle = Win32MemoryMethods.GetCurrentProcess();

                    // supported only on Windows 10 22H2 with September 2023 cumulative update or Windows 11 22H2 with September 2023 cumulative update and above
                    var expectedSize = (uint)Marshal.SizeOf(typeof(Win32MemoryMethods.PROCESS_MEMORY_COUNTERS_EX2));
                    Win32MemoryMethods.GetProcessMemoryInfo(process.Handle, out var memCounters, expectedSize);
                    WindowsSupportsMemoryCountersEx2 = expectedSize == memCounters.cb;

                    if (Win32MemoryMethods.GetPhysicallyInstalledSystemMemory(out var installedMemoryInKb))
                    {
                        // The amount of physical memory retrieved by the GetPhysicallyInstalledSystemMemory function
                        // must be equal to or greater than the amount reported by the GlobalMemoryStatusEx function
                        // if it is less, the SMBIOS data is malformed and the function fails with ERROR_INVALID_DATA.
                        // Malformed SMBIOS data may indicate a problem with the user's computer.
                        WindowsInstalledMemory = new Size(installedMemoryInKb, SizeUnit.Kilobytes);
                    }

                }
            }

            TotalPhysicalMemory = GetMemoryInfo().TotalPhysicalMemory;
        }

        private static bool _failedToGetAvailablePhysicalMemory;

        private static readonly MemoryInfoResult FailedResult = new MemoryInfoResult
        {
            AvailableMemory = new Size(256, SizeUnit.Megabytes),
            AvailableMemoryForProcessing = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
            TotalCommittableMemory = new Size(384, SizeUnit.Megabytes),// also include "page file"
            CurrentCommitCharge = new Size(256, SizeUnit.Megabytes),
            InstalledMemory = new Size(256, SizeUnit.Megabytes)
        };

        public static bool DisableEarlyOutOfMemoryCheck =
            string.Equals(Environment.GetEnvironmentVariable("RAVEN_DISABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

        public static bool EnableEarlyOutOfMemoryCheck =
            string.Equals(Environment.GetEnvironmentVariable("RAVEN_ENABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

        public static bool EnableEarlyOutOfMemoryChecks = false; // we don't want this to run on the clients

        private static float _minimumFreeCommittedMemoryPercentage = 0.05f;
        private static Size _maxFreeCommittedMemoryToKeep = new Size(128, SizeUnit.Megabytes);
        private static Size _lowMemoryCommitLimitInMb = new Size(512, SizeUnit.Megabytes);

        internal struct ProcMemInfoResults
        {
            public Size AvailableMemory;
            public Size TotalMemory;
            public Size Commited;
            public Size CommitLimit;
            public Size AvailableMemoryForProcessing;
            public Size SharedCleanMemory;
            public Size TotalDirty;
            public Size TotalSwap;
            public Size WorkingSetSwap;
        }

        internal static ConcurrentSet<StrongReference<Func<long>>> DirtyMemoryObjects = new ConcurrentSet<StrongReference<Func<long>>>();

        public static void SetFreeCommittedMemory(float minimumFreeCommittedMemoryPercentage, Size maxFreeCommittedMemoryToKeep, Size lowMemoryCommitLimitInMb)
        {
            if (minimumFreeCommittedMemoryPercentage <= 0)
                throw new ArgumentException($"MinimumFreeCommittedMemory must be positive, but was: {minimumFreeCommittedMemoryPercentage}");

            _minimumFreeCommittedMemoryPercentage = minimumFreeCommittedMemoryPercentage;
            _maxFreeCommittedMemoryToKeep = maxFreeCommittedMemoryToKeep;
            _lowMemoryCommitLimitInMb = lowMemoryCommitLimitInMb;
        }

        private static int _isAboutToRunOutOfMemory;

        public static void AssertNotAboutToRunOutOfMemory()
        {
            if (_isAboutToRunOutOfMemory == 0)
                return;

            // The background thread flagged potential memory pressure.
            // Re-check with fresh data so allocations can still proceed
            // if memory has been freed since the last background check.
            var memInfo = GetEarlyOutOfMemoryInfo();
            if (IsEarlyOutOfMemoryInternal(memInfo, earlyOutOfMemoryWarning: false, out _))
                ThrowInsufficientMemory(GetMemoryInfo());
        }

        internal static void UpdateAboutToRunOutOfMemoryFlag()
        {
            int newValue = 0;

            if (EnableEarlyOutOfMemoryChecks &&
                DisableEarlyOutOfMemoryCheck == false &&
                (PlatformDetails.RunningOnPosix == false ||
                 EnableEarlyOutOfMemoryCheck))             // we only _need_ this check on Windows
            {                                              // but we want to enable this manually if needed
                var memInfo = GetEarlyOutOfMemoryInfo();
                if (IsEarlyOutOfMemoryInternal(memInfo, earlyOutOfMemoryWarning: false, out _))
                    newValue = 1;
            }

            if (newValue != _isAboutToRunOutOfMemory)
                Interlocked.Exchange(ref _isAboutToRunOutOfMemory, newValue);
        }

        internal static bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
                EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
            {
                commitChargeThreshold = Size.Zero;
                return false;
            }

            return IsEarlyOutOfMemoryInternal(new LightWeightMemoryInfoResult
            {
                AvailableMemory = memInfo.AvailableMemory,
                CurrentCommitCharge = memInfo.CurrentCommitCharge,
                TotalCommittableMemory = memInfo.TotalCommittableMemory
            }, earlyOutOfMemoryWarning: true, out commitChargeThreshold);
        }

        private static bool IsEarlyOutOfMemoryInternal(LightWeightMemoryInfoResult memInfo, bool earlyOutOfMemoryWarning, out Size commitChargeThreshold)
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

                commitChargeThreshold = GetMinCommittedToKeep(TotalPhysicalMemory);
                overage =
                    commitChargeThreshold +                                    //extra to keep free
                    (TotalPhysicalMemory - memInfo.AvailableMemory);   //actually in use now

                return overage >= TotalPhysicalMemory;
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

        [DoesNotReturn]
        private static void ThrowInsufficientMemory(MemoryInfoResult memInfo)
        {
            LowMemoryNotification.Instance.SimulateLowMemoryNotification();

            throw new EarlyOutOfMemoryException($"The amount of available memory to commit on the system is low. " +
                                                MemoryUtils.GetExtendedMemoryInfo(memInfo, GetDirtyMemoryState()), memInfo);

        }

        public static (long Rss, long Swap) GetMemoryUsageFromProcStatus()
        {
            var path = $"/proc/{ProcessId}/status";

            try
            {
                using (var bufferedReader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(path))
                {
                    bufferedReader.ReadFileIntoBuffer();
                    var vmrss = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(VmRss);
                    var vmswap = bufferedReader.ExtractNumericValueFromKeyValuePairsFormattedFile(VmSwap);
                    
                    // value is in KB, we need to return bytes
                    return (vmrss * 1024, vmswap * 1024); 
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read value from {path}", ex);
                return (-1, -1);
            }
        }

        internal static MemoryInfoResult GetMemoryInformationUsingOneTimeSmapsReader()
        {
            ISmapsReader smapsReader = null;
            byte[][] buffers = null;
            try
            {
                if (PlatformDetails.RunningOnLinux)
                {
                    var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsFactory.BufferSize);
                    var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsFactory.BufferSize);
                    buffers = new[] { buffer1, buffer2 };
                    smapsReader = SmapsFactory.CreateSmapsReader(new[] { buffer1, buffer2 });
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

        private static bool GetFromProcMemInfo(ISmapsReader smapsReader, ref ProcMemInfoResults procMemInfoResults)
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

                    var totalClean = new Size(0, SizeUnit.Kilobytes);
                    var totalDirty = new Size(0, SizeUnit.Bytes);
                    var sharedCleanMemory = new Size(0, SizeUnit.Bytes);
                    var swapWorkingSet = new Size(0, SizeUnit.Bytes);
                    if (smapsReader != null)
                    {
                        var result = smapsReader.CalculateMemUsageFromSmaps<SmapsReaderNoAllocResults>();
                        totalClean.Add(result.SharedClean, SizeUnit.Bytes);
                        totalClean.Add(result.PrivateClean, SizeUnit.Bytes);
                        sharedCleanMemory.Set(result.SharedClean, SizeUnit.Bytes);
                        totalDirty.Add(result.TotalDirty, SizeUnit.Bytes);
                        swapWorkingSet.Add(result.Swap, SizeUnit.Bytes);
                    }

                    procMemInfoResults.AvailableMemory = new Size(memFreeInKb, SizeUnit.Kilobytes);
                    procMemInfoResults.TotalMemory = new Size(totalMemInKb, SizeUnit.Kilobytes);
                    procMemInfoResults.Commited = new Size(commitedInKb, SizeUnit.Kilobytes);

                    // on Linux, we use the swap + ram as the commit limit, because the actual limit
                    // is dependent on many different factors
                    procMemInfoResults.CommitLimit = new Size(totalMemInKb + swapTotalInKb, SizeUnit.Kilobytes);

                    // AvailableMemoryForProcessing: AvailableMemory actually does add reclaimable memory (divided by 2), so if AvailableMemory is equal or lower then the _real_ available memory
                    // If it is lower the the real value because of RavenDB's Clean memory - then we use 'totalClean' as reference
                    // Otherwise - either it is correct value, or it is lower because of (clean or dirty memory of) another process
                    var availableMemoryForProcessing = new Size(Math.Max(memAvailableInKb, memFreeInKb), SizeUnit.Kilobytes);
                    procMemInfoResults.AvailableMemoryForProcessing = Size.Max(availableMemoryForProcessing, totalClean);

                    procMemInfoResults.SharedCleanMemory = sharedCleanMemory;
                    procMemInfoResults.TotalDirty = totalDirty;
                    procMemInfoResults.TotalSwap = new Size(swapTotalInKb, SizeUnit.Kilobytes);
                    procMemInfoResults.WorkingSetSwap = swapWorkingSet;
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read value from {path}", ex);

                return false;
            }

            return true;
        }

        internal static MemoryInfoResult GetMemoryInfo(ISmapsReader smapsReader = null, bool extended = false)
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

                MemoryInfoResult result;
                {
                    if (PlatformDetails.RunningOnPosix == false)
                        result = GetMemoryInfoWindows();
                    else if (PlatformDetails.RunningOnMacOsx)
                        using (var process = extended ? Process.GetCurrentProcess() : null)
                            result = GetMemoryInfoMacOs(process, extended);
                    else
                        result = GetMemoryInfoLinux(smapsReader, extended);
                }

                return result;

            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                _failedToGetAvailablePhysicalMemory = true;
                return FailedResult;
            }
        }

        public static long GetTotalScratchAllocatedMemoryInBytes()
        {
            long totalScratchAllocated = 0;
            foreach (var scratchGetAllocated in DirtyMemoryObjects)
            {
                totalScratchAllocated += scratchGetAllocated.Value?.Invoke() ?? 0;
            }

            return totalScratchAllocated;
        }

        private static MemoryInfoResult GetMemoryInfoLinux(ISmapsReader smapsReader, bool extended)
        {
            var fromProcMemInfo = new ProcMemInfoResults();
            GetFromProcMemInfo(smapsReader, ref fromProcMemInfo);

            var totalPhysicalMemoryInBytes = fromProcMemInfo.TotalMemory.GetValue(SizeUnit.Bytes);

            var cgroupMemoryLimit = CGroupHelper.CGroup.GetPhysicalMemoryLimit();
            var cgroupMaxMemoryUsage = CGroupHelper.CGroup.GetMaxMemoryUsage();
            // here we need to deal with _soft_ limit, so we'll take the largest of these values
            var maxMemoryUsage = Math.Max(cgroupMemoryLimit ?? 0, cgroupMaxMemoryUsage ?? 0);
            var constrainedByCgroups = maxMemoryUsage != 0 && maxMemoryUsage <= totalPhysicalMemoryInBytes;
            if (constrainedByCgroups)
            {
                // running in a limited cgroup
                var commitedMemoryInBytes = 0L;
                var cgroupMemoryUsage = LowMemoryNotification.Instance.UseTotalDirtyMemInsteadOfMemUsage // RDBS-45
                    ? fromProcMemInfo.TotalDirty.GetValue(SizeUnit.Bytes)
                    : CGroupHelper.CGroup.GetPhysicalMemoryUsage();

                if (cgroupMemoryUsage != null)
                {
                    commitedMemoryInBytes = cgroupMemoryUsage.Value;
                    fromProcMemInfo.Commited.Set(commitedMemoryInBytes, SizeUnit.Bytes);
                    var availableMemory = Math.Min(maxMemoryUsage - cgroupMemoryUsage.Value, fromProcMemInfo.AvailableMemory.GetValue(SizeUnit.Bytes));
                    fromProcMemInfo.AvailableMemory.Set(availableMemory, SizeUnit.Bytes);
                    var realAvailable = maxMemoryUsage - cgroupMemoryUsage.Value + fromProcMemInfo.SharedCleanMemory.GetValue(SizeUnit.Bytes);
                    if (realAvailable < 0)
                        realAvailable = 0;
                    realAvailable = Math.Min(realAvailable, fromProcMemInfo.AvailableMemoryForProcessing.GetValue(SizeUnit.Bytes));
                    fromProcMemInfo.AvailableMemoryForProcessing.Set(realAvailable, SizeUnit.Bytes);
                }

                fromProcMemInfo.TotalMemory.Set(maxMemoryUsage, SizeUnit.Bytes);
                fromProcMemInfo.CommitLimit.Set(Math.Max(maxMemoryUsage, commitedMemoryInBytes), SizeUnit.Bytes);
            }

            var workingSet = new Size(0, SizeUnit.Bytes);
            var swapUsage = new Size(0, SizeUnit.Bytes);
            if (smapsReader != null)
            {
                // extended info is needed

                var procStatus = GetMemoryUsageFromProcStatus();
                workingSet.Set(procStatus.Rss, SizeUnit.Bytes);
                swapUsage.Set(procStatus.Swap, SizeUnit.Bytes);
            }

            return new MemoryInfoResult
            {
                TotalCommittableMemory = fromProcMemInfo.CommitLimit,
                CurrentCommitCharge = fromProcMemInfo.Commited,

                AvailableMemory = fromProcMemInfo.AvailableMemory,
                AvailableMemoryForProcessing = fromProcMemInfo.AvailableMemoryForProcessing,
                SharedCleanMemory = fromProcMemInfo.SharedCleanMemory,
                TotalPhysicalMemory = fromProcMemInfo.TotalMemory,
                InstalledMemory = fromProcMemInfo.TotalMemory,
                WorkingSet = workingSet,
                
                TotalSwapSize = fromProcMemInfo.TotalSwap,
                TotalSwapUsage = swapUsage,
                WorkingSetSwapUsage = fromProcMemInfo.WorkingSetSwap,
                
                IsExtended = extended,
                Remarks = constrainedByCgroups ? "Memory constrained by cgroups limits" :  null
            };
        }

        private static unsafe MemoryInfoResult GetMemoryInfoMacOs(Process process, bool extended)
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

            ulong compressedMemoryInBytes = 0;
            int size = sizeof(ulong);

            if (macSyscall.sysctlbyname("vm.compressor_bytes_used", &compressedMemoryInBytes, &size, null, UIntPtr.Zero) != 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to read compressed memory in bytes info from macOS using sysctlbyname, error code was: " + Marshal.GetLastWin32Error());
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
            var availableMemory = new Size((vmStats.FreePagesCount + vmStats.InactivePagesCount + vmStats.PurgeablePagesCount + vmStats.SpeculativePagesCount) * pageSize, SizeUnit.Bytes);

            // there is no commited memory value in OSX,
            // this is an approximation: wired + active + swap used
            var commitedMemoryInBytes = (vmStats.WirePagesCount + vmStats.ActivePagesCount) * pageSize + (long)compressedMemoryInBytes + (long)swapu.xsu_used;
            var commitedMemory = new Size(commitedMemoryInBytes, SizeUnit.Bytes);

            // commit limit: physical memory + swap
            var commitLimit = new Size((long)(physicalMemory + swapu.xsu_total), SizeUnit.Bytes);

            var availableMemoryForProcessing = availableMemory; // mac (unlike other linux distros) does calculate accurate available memory
            var workingSet = new Size(process?.WorkingSet64 ?? 0, SizeUnit.Bytes);

            return new MemoryInfoResult
            {
                TotalCommittableMemory = commitLimit,
                CurrentCommitCharge = commitedMemory,

                AvailableMemory = availableMemory,
                AvailableMemoryForProcessing = availableMemoryForProcessing,
                SharedCleanMemory = Size.Zero,
                TotalPhysicalMemory = totalPhysicalMemory,
                InstalledMemory = totalPhysicalMemory,
                WorkingSet = workingSet,
                IsExtended = extended
            };
        }

        private static bool _reportedQueryJobObjectFailure = false;

        private static unsafe MemoryInfoResult GetMemoryInfoWindows()
        {
            // windows
            var memoryStatus = new Win32MemoryMethods.MemoryStatusEx
            {
                dwLength = (uint)sizeof(Win32MemoryMethods.MemoryStatusEx)
            };

            if (Win32MemoryMethods.GlobalMemoryStatusEx(&memoryStatus) == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                return FailedResult;
            }

            var sharedCleanInBytes = GetSharedCleanInBytes(out long workingSet, out long pageFileUsage);
            long memoryStatusUllAvailPhys = (long)memoryStatus.ullAvailPhys;
            long totalPageFile = (long)memoryStatus.ullTotalPageFile;
            long availPageFile = (long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile);
            var availableMemoryForProcessingInBytes = memoryStatusUllAvailPhys + sharedCleanInBytes;

            string remarks = null;
            if (Win32MemoryMethods.IsProcessInJob(ProcessHandle, IntPtr.Zero, out var isInJob) && isInJob)
            {
                Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = default;
                if (Win32MemoryMethods.QueryInformationJobObject(IntPtr.Zero,
                        Win32MemoryMethods.JOBOBJECTINFOCLASS.ExtendedLimitInformation, (void*)&limits,
                        sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
                    out int limitsOutputSize) == false || 
                    limitsOutputSize != sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION))
            {
                if (_reportedQueryJobObjectFailure == false && Logger.IsWarnEnabled)
                {
                    _reportedQueryJobObjectFailure = true;
                    Logger.Warn(
                            $"Failure when trying to query job object information info from Windows, error code is: {Marshal.GetLastWin32Error()}. Output size: {limitsOutputSize} instead of {sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION)}!");
                }
            }
            else
            {
                long maxSize = long.MaxValue;
                if (limits.BasicLimitInformation.MaximumWorkingSetSize != UIntPtr.Zero)
                {
                    maxSize = (long)limits.BasicLimitInformation.MaximumWorkingSetSize;
                }

                if (limits.ProcessMemoryLimit != UIntPtr.Zero)
                {
                    maxSize = Math.Min(maxSize, (long)limits.ProcessMemoryLimit);
                }
                
                if (limits.JobMemoryLimit != UIntPtr.Zero)
                {
                    maxSize = Math.Min(maxSize, (long)limits.ProcessMemoryLimit);
                }

                if (maxSize != long.MaxValue)
                {
                        availableMemoryForProcessingInBytes = Math.Max(maxSize - workingSet, 0);
                        availPageFile = Math.Max(maxSize - workingSet, 0);
                    totalPageFile = maxSize;
                    memoryStatusUllAvailPhys = Math.Min(availableMemoryForProcessingInBytes, memoryStatusUllAvailPhys);
                    remarks = "Memory limited by Job Object limits";
                }
            }
            }

            return new MemoryInfoResult
            {
                Remarks = remarks,
                TotalCommittableMemory = new Size(totalPageFile, SizeUnit.Bytes),
                CurrentCommitCharge = new Size(availPageFile, SizeUnit.Bytes),
                AvailableMemory = new Size(memoryStatusUllAvailPhys, SizeUnit.Bytes),
                AvailableMemoryForProcessing = new Size(availableMemoryForProcessingInBytes, SizeUnit.Bytes),
                SharedCleanMemory = new Size(sharedCleanInBytes, SizeUnit.Bytes),
                TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                InstalledMemory = WindowsInstalledMemory ?? new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                WorkingSet = new Size(workingSet, SizeUnit.Bytes),
                IsExtended = true,
                TotalSwapUsage = new Size(pageFileUsage, SizeUnit.Bytes)
            };
        }

        public static long GetSharedCleanInBytes(out long workingSet, out long pageFileUsage)
        {
            // The used space in scratch buffers represents dirty memory. While we cannot precisely determine how much
            // of it resides in physical memory, this approach provides a highly reliable approximation of the shared
            // dirty memory footprint.
            var sharedDirty = GetTotalScratchAllocatedMemoryInBytes();

            (long workingSetInBytes, long pageFileUsageInBytes, long? privateUsageInBytes) = GetProcessMemoryInfoForWindows();
            var privateUsage = privateUsageInBytes ?? AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes() + AbstractLowMemoryMonitor.GetManagedMemoryInBytes();

            workingSet = workingSetInBytes;
            pageFileUsage = pageFileUsageInBytes;

            return Math.Max(0, workingSet - privateUsage - sharedDirty);
        }

        private static (long WorkingSetInBytes, long PageFileUsageInBytes, long? PrivateUsageInBytes) GetProcessMemoryInfoForWindows()
            {
            long workingSet;
            long pageFileUsage;
            long? privateUsage;

            if (WindowsSupportsMemoryCountersEx2)
                {
                if (Win32MemoryMethods.GetProcessMemoryInfo(ProcessHandle, out Win32MemoryMethods.PROCESS_MEMORY_COUNTERS_EX2 memCounters, (uint)Marshal.SizeOf(typeof(Win32MemoryMethods.PROCESS_MEMORY_COUNTERS_EX2))) == false)
                    {
                    throw new InvalidOperationException("Failure when trying to read memory using GetProcessMemoryInfo, error code is: " + Marshal.GetLastWin32Error());
                    }

                workingSet = (long)memCounters.WorkingSetSize;
                pageFileUsage = (long)memCounters.PagefileUsage;
                privateUsage = (long)memCounters.PrivateWorkingSetSize;
            }
            else
        {
                if (Win32MemoryMethods.GetProcessMemoryInfoLegacy(ProcessHandle, out Win32MemoryMethods.PROCESS_MEMORY_COUNTERS_EX memCounters, (uint)Marshal.SizeOf(typeof(Win32MemoryMethods.PROCESS_MEMORY_COUNTERS_EX))) == false)
            {
                    throw new InvalidOperationException("Failure when trying to read memory using the legacy GetProcessMemoryInfo, error code is: " + Marshal.GetLastWin32Error());
            }

                workingSet = (long)memCounters.WorkingSetSize;
                pageFileUsage = (long)memCounters.PagefileUsage;
                privateUsage = null;
        }

            return (workingSet, pageFileUsage, privateUsage);
        }

        public static long GetWorkingSetInBytes()
        {
            if (PlatformDetails.RunningOnLinux)
                return GetMemoryUsageFromProcStatus().Rss;

            if (PlatformDetails.RunningOnWindows)
            {
                (long workingSetInBytes, _, _) = GetProcessMemoryInfoForWindows();
                return workingSetInBytes;
            }

            using (var currentProcess = Process.GetCurrentProcess())
            {
                return currentProcess.WorkingSet64;
            }
        }

        internal static DirtyMemoryState GetDirtyMemoryState()
        {
            var totalScratchMemory = new Size(GetTotalScratchAllocatedMemoryInBytes(), SizeUnit.Bytes);

            return new DirtyMemoryState
            {
                IsHighDirty = totalScratchMemory >
                              TotalPhysicalMemory * LowMemoryNotification.Instance.TemporaryDirtyMemoryAllowedPercentage,
                TotalDirty = totalScratchMemory
            };
        }
    }

    public sealed class EarlyOutOfMemoryException : SystemException
    {
        public EarlyOutOfMemoryException()
        {
        }

        public EarlyOutOfMemoryException(string message, MemoryInfoResult memoryInfo) : base(message)
        {
            MemoryInfo = memoryInfo;
        }

        public EarlyOutOfMemoryException(string message, Exception inner) : base(message, inner)
        {
        }

        public MemoryInfoResult? MemoryInfo { get; }
    }
}
