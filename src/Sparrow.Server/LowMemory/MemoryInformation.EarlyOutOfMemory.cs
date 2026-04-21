using System;
using System.Runtime.InteropServices;
using Sparrow.Platform;
using Sparrow.Server.Platform.Win32;

namespace Sparrow.Server.LowMemory;

public static partial class MemoryInformation
{
    private static readonly LightWeightMemoryInfoResult FailedLightWeightMemoryResult = new()
    {
        AvailableMemory = new Size(256, SizeUnit.Megabytes),
        TotalCommittableMemory = new Size(384, SizeUnit.Megabytes),
        CurrentCommitCharge = new Size(256, SizeUnit.Megabytes),
    };

    private static LightWeightMemoryInfoResult GetEarlyOutOfMemoryInfo()
    {
        if (_failedToGetAvailablePhysicalMemory)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
            return FailedLightWeightMemoryResult;
        }

        try
        {
            LightWeightMemoryInfoResult result;

            if (PlatformDetails.RunningOnPosix == false)
            {
                result = GetEarlyOutOfMemoryInfoWindows();
            }
            else if (PlatformDetails.RunningOnMacOsx)
            {
                var info = GetMemoryInfoMacOs(process: null, extended: false);
                result = new LightWeightMemoryInfoResult
                {
                    AvailableMemory = info.AvailableMemory,
                    CurrentCommitCharge = info.CurrentCommitCharge,
                    TotalCommittableMemory = info.TotalCommittableMemory
                };
            }
            else
            {
                var info = GetMemoryInfoLinux(smapsReader: null, extended: false);
                result = new LightWeightMemoryInfoResult
                {
                    AvailableMemory = info.AvailableMemory,
                    CurrentCommitCharge = info.CurrentCommitCharge,
                    TotalCommittableMemory = info.TotalCommittableMemory
                };
            }

            return result;

        }
        catch (Exception e)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
            _failedToGetAvailablePhysicalMemory = true;
            return FailedLightWeightMemoryResult;
        }
    }

    private static unsafe LightWeightMemoryInfoResult GetEarlyOutOfMemoryInfoWindows()
    {
        var memoryStatus = new Win32MemoryMethods.MemoryStatusEx
        {
            dwLength = (uint)sizeof(Win32MemoryMethods.MemoryStatusEx)
        };

        if (Win32MemoryMethods.GlobalMemoryStatusEx(&memoryStatus) == false)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
            return FailedLightWeightMemoryResult;
        }

        long memoryStatusUllAvailPhys = (long)memoryStatus.ullAvailPhys;
        long totalPageFile = (long)memoryStatus.ullTotalPageFile;
        long availPageFile = (long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile);

        if (Win32MemoryMethods.IsProcessInJob(ProcessHandle, IntPtr.Zero, out var isInJob) && isInJob)
        {
            Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = default;
            if (Win32MemoryMethods.QueryInformationJobObject(IntPtr.Zero,
                    Win32MemoryMethods.JOBOBJECTINFOCLASS.ExtendedLimitInformation, (void*)&limits,
                    sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
                    out int limitsOutputSize) == false ||
                limitsOutputSize != sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION))
            {
                if (_reportedQueryJobObjectFailure == false && Logger.IsInfoEnabled)
                {
                    _reportedQueryJobObjectFailure = true;
                    Logger.Info(
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
                    maxSize = Math.Min(maxSize, (long)limits.JobMemoryLimit);
                }

                if (maxSize != long.MaxValue)
                {
                    (long workingSetInBytes, long _, long? _) = GetProcessMemoryInfoForWindows();
                    var availableMemoryForProcessingInBytes = Math.Max(maxSize - workingSetInBytes, 0);
                    availPageFile = Math.Max(maxSize - workingSetInBytes, 0);
                    totalPageFile = maxSize;
                    memoryStatusUllAvailPhys = Math.Min(availableMemoryForProcessingInBytes, memoryStatusUllAvailPhys);
                }
            }
        }

        return new LightWeightMemoryInfoResult
        {
            TotalCommittableMemory = new Size(totalPageFile, SizeUnit.Bytes),
            CurrentCommitCharge = new Size(availPageFile, SizeUnit.Bytes),
            AvailableMemory = new Size(memoryStatusUllAvailPhys, SizeUnit.Bytes),
        };
    }

    public struct LightWeightMemoryInfoResult
    {
        public Size CurrentCommitCharge { get; set; }
        public Size TotalCommittableMemory { get; set; }
        public Size AvailableMemory { get; set; }
    }
}
