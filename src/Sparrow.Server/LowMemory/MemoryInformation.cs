﻿using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Posix.macOS;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Server");

        private static readonly byte[] VmRss = Encoding.UTF8.GetBytes("VmRSS:");
        private static readonly byte[] VmSwap = Encoding.UTF8.GetBytes("VmSwap:");
        private static readonly byte[] MemAvailable = Encoding.UTF8.GetBytes("MemAvailable:");
        private static readonly byte[] MemFree = Encoding.UTF8.GetBytes("MemFree:");
        private static readonly byte[] MemTotal = Encoding.UTF8.GetBytes("MemTotal:");
        private static readonly byte[] SwapTotal = Encoding.UTF8.GetBytes("SwapTotal:");
        private static readonly byte[] Committed_AS = Encoding.UTF8.GetBytes("Committed_AS:");

        private static readonly int ProcessId;

        public static readonly Size TotalPhysicalMemory;

        static MemoryInformation()
        {
            using (var process = Process.GetCurrentProcess())
                ProcessId = process.Id;

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

            throw new EarlyOutOfMemoryException($"The amount of available memory to commit on the system is low. " +
                                                MemoryUtils.GetExtendedMemoryInfo(memInfo, GetDirtyMemoryState()), memInfo);

        }

        public enum JOBOBJECTINFOCLASS
        {
            AssociateCompletionPortInformation = 7,
            BasicLimitInformation = 2,
            BasicUIRestrictions = 4,
            EndOfJobTimeInformation = 6,
            ExtendedLimitInformation = 9,
            SecurityLimitInformation = 5,
            GroupInformation = 11
        }
        
        [StructLayout(LayoutKind.Sequential)]
        struct JOBOBJECT_BASIC_LIMIT_INFORMATION
        {
            public Int64 PerProcessUserTimeLimit;
            public Int64 PerJobUserTimeLimit;
            public JOBOBJECTLIMIT LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public UInt32 ActiveProcessLimit;
            public Int64 Affinity;
            public UInt32 PriorityClass;
            public UInt32 SchedulingClass;
        }
        
        [Flags]
        public enum JOBOBJECTLIMIT : uint
        {
            // Basic Limits
            Workingset = 0x00000001,
            ProcessTime = 0x00000002,
            JobTime = 0x00000004,
            ActiveProcess = 0x00000008,
            Affinity = 0x00000010,
            PriorityClass = 0x00000020,
            PreserveJobTime = 0x00000040,
            SchedulingClass = 0x00000080,

            // Extended Limits
            ProcessMemory = 0x00000100,
            JobMemory = 0x00000200,
            DieOnUnhandledException = 0x00000400,
            BreakawayOk = 0x00000800,
            SilentBreakawayOk = 0x00001000,
            KillOnJobClose = 0x00002000,
            SubsetAffinity = 0x00004000,

            // Notification Limits
            JobReadBytes = 0x00010000,
            JobWriteBytes = 0x00020000,
            RateControl = 0x00040000,
        }
        
        [StructLayout(LayoutKind.Sequential)]
        struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [StructLayout(LayoutKind.Sequential)]
        struct IO_COUNTERS
        {
            public UInt64 ReadOperationCount;
            public UInt64 WriteOperationCount;
            public UInt64 OtherOperationCount;
            public UInt64 ReadTransferCount;
            public UInt64 WriteTransferCount;
            public UInt64 OtherTransferCount;
        }
        
        
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll")]
        public static extern unsafe bool QueryInformationJobObject(IntPtr hJob, JOBOBJECTINFOCLASS JobObjectInformationClass, void* lpJobObjectInformation,
            int cbJobObjectInformationLength, out int lpReturnLength);


        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern unsafe bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetPhysicallyInstalledSystemMemory(out long totalMemoryInKb);

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

        public static MemoryInfoResult GetMemoryInformationUsingOneTimeSmapsReader()
        {
            AbstractSmapsReader smapsReader = null;
            byte[][] buffers = null;
            try
            {
                if (PlatformDetails.RunningOnLinux)
                {
                    var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    buffers = new[] { buffer1, buffer2 };
                    smapsReader = AbstractSmapsReader.CreateSmapsReader(new[] { buffer1, buffer2 });
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

        private static bool GetFromProcMemInfo(AbstractSmapsReader smapsReader, ref ProcMemInfoResults procMemInfoResults)
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

        internal static MemoryInfoResult GetMemoryInfo(AbstractSmapsReader smapsReader = null, bool extended = false)
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
                using (var process = extended ? Process.GetCurrentProcess() : null)
                {
                    if (PlatformDetails.RunningOnPosix == false)
                        result = GetMemoryInfoWindows(process, extended);
                    else if (PlatformDetails.RunningOnMacOsx)
                        result = GetMemoryInfoMacOs(process, extended);
                    else
                        result = GetMemoryInfoLinux(smapsReader, extended);
                }

                return result;

            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
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

        private static MemoryInfoResult GetMemoryInfoLinux(AbstractSmapsReader smapsReader, bool extended)
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
            var availableMemory = new Size((vmStats.FreePagesCount + vmStats.InactivePagesCount) * pageSize, SizeUnit.Bytes);

            // there is no commited memory value in OSX,
            // this is an approximation: wired + active + swap used
            var commitedMemoryInBytes = (vmStats.WirePagesCount + vmStats.ActivePagesCount) * pageSize + (long)swapu.xsu_used;
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

        private static unsafe MemoryInfoResult GetMemoryInfoWindows(Process process, bool extended)
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

            var sharedCleanInBytes = GetSharedCleanInBytes(process);
            long memoryStatusUllAvailPhys = (long)memoryStatus.ullAvailPhys;
            long totalPageFile = (long)memoryStatus.ullTotalPageFile;
            long availPageFile = (long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile);
            var availableMemoryForProcessingInBytes = memoryStatusUllAvailPhys + sharedCleanInBytes;

            string remarks = null;
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = default;
            if (QueryInformationJobObject(IntPtr.Zero, 
                    JOBOBJECTINFOCLASS.ExtendedLimitInformation, (void*)&limits, 
                    sizeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
                    out int limitsOutputSize) == false || 
                limitsOutputSize != sizeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION))
            {
                if (_reportedQueryJobObjectFailure == false && Logger.IsInfoEnabled)
                {
                    _reportedQueryJobObjectFailure = true;
                    Logger.Info(
                        $"Failure when trying to query job object information info from Windows, error code is: {Marshal.GetLastWin32Error()}. Output size: {limitsOutputSize} instead of {sizeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION)}!");
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
                    long workingSet64;
                    if (process == null)
                    {
                        using (var p = Process.GetCurrentProcess())
                        {
                            workingSet64 = p.WorkingSet64;
                        }
                    }
                    else
                    {
                        workingSet64 = process.WorkingSet64;
                    }

                    availableMemoryForProcessingInBytes = Math.Max(maxSize - workingSet64, 0);
                    availPageFile = Math.Max(maxSize - workingSet64, 0);
                    totalPageFile = maxSize;
                    memoryStatusUllAvailPhys = Math.Min(availableMemoryForProcessingInBytes, memoryStatusUllAvailPhys);
                    remarks = "Memory limited by Job Object limits";
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
                InstalledMemory = fetchedInstalledMemory ?
                    new Size(installedMemoryInKb, SizeUnit.Kilobytes) :
                    new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                WorkingSet = new Size(process?.WorkingSet64 ?? 0, SizeUnit.Bytes),
                IsExtended = extended,
                TotalSwapUsage = new Size(process?.PagedMemorySize64 ?? 0, SizeUnit.Bytes)
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

        public static long GetWorkingSetInBytes()
        {
            if (PlatformDetails.RunningOnLinux)
                return GetMemoryUsageFromProcStatus().Rss;

            using (var currentProcess = Process.GetCurrentProcess())
            {
                return currentProcess.WorkingSet64;
            }
        }

        public static DirtyMemoryState GetDirtyMemoryState()
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

    public class EarlyOutOfMemoryException : SystemException
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
