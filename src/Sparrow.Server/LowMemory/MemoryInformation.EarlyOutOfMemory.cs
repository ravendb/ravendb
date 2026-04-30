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

        long availableMemoryForProcessingInBytes = memoryStatusUllAvailPhys;
        if (Win32MemoryMethods.IsProcessInJob(ProcessHandle, IntPtr.Zero, out var isInJob) && isInJob)
        {
            (long workingSet, long _, long? _) = GetProcessMemoryInfoForWindows();
            TryApplyJobObjectMemoryLimits(workingSet, ref memoryStatusUllAvailPhys, ref totalPageFile, ref availPageFile, ref availableMemoryForProcessingInBytes);
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
