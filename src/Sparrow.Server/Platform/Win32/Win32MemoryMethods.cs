using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow.Server.Platform.Win32
{
    public static unsafe class Win32MemoryMethods
    {
        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetPhysicallyInstalledSystemMemory(out long totalMemoryInKb);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        [DllImport("psapi.dll", SetLastError = true)]
        public static extern bool GetProcessMemoryInfo(IntPtr hProcess, out PROCESS_MEMORY_COUNTERS_EX2 counters, uint size);

        [DllImport("psapi.dll", SetLastError = true, EntryPoint = "GetProcessMemoryInfo")]
        public static extern bool GetProcessMemoryInfoLegacy(IntPtr hProcess, out PROCESS_MEMORY_COUNTERS_EX counters, uint size);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool IsProcessInJob(IntPtr hProcess, IntPtr hJob, out bool isInJob);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll")]
        public static extern bool QueryInformationJobObject(IntPtr hJob, JOBOBJECTINFOCLASS JobObjectInformationClass, void* lpJobObjectInformation,
            int cbJobObjectInformationLength, out int lpReturnLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetCurrentProcess();

        [DllImport("psapi.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool QueryWorkingSetEx(IntPtr hProcess, byte* pv, uint cb);

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

        //https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex2
        [StructLayout(LayoutKind.Sequential)]
        public struct PROCESS_MEMORY_COUNTERS_EX2
        {
            public uint cb;
            public uint PageFaultCount;
            public ulong PeakWorkingSetSize;
            public ulong WorkingSetSize;
            public ulong QuotaPeakPagedPoolUsage;
            public ulong QuotaPagedPoolUsage;
            public ulong QuotaPeakNonPagedPoolUsage;
            public ulong QuotaNonPagedPoolUsage;
            public ulong PagefileUsage;
            public ulong PeakPagefileUsage;
            public ulong PrivateUsage;
            public ulong PrivateWorkingSetSize;
            public ulong SharedCommitUsage;
        }

        //https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex
        [StructLayout(LayoutKind.Sequential)]
        public struct PROCESS_MEMORY_COUNTERS_EX
        {
            public uint cb;
            public uint PageFaultCount;
            public ulong PeakWorkingSetSize;
            public ulong WorkingSetSize;
            public ulong QuotaPeakPagedPoolUsage;
            public ulong QuotaPagedPoolUsage;
            public ulong QuotaPeakNonPagedPoolUsage;
            public ulong QuotaNonPagedPoolUsage;
            public ulong PagefileUsage;
            public ulong PeakPagefileUsage;
            public ulong PrivateUsage;
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
        public struct JOBOBJECT_BASIC_LIMIT_INFORMATION
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
        public struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct IO_COUNTERS
        {
            public UInt64 ReadOperationCount;
            public UInt64 WriteOperationCount;
            public UInt64 OtherOperationCount;
            public UInt64 ReadTransferCount;
            public UInt64 WriteTransferCount;
            public UInt64 OtherTransferCount;
        }

        // ReSharper disable once InconsistentNaming
        struct PPSAPI_WORKING_SET_EX_INFORMATION
        {
            // ReSharper disable once NotAccessedField.Local - it accessed via ptr
            public byte* VirtualAddress;
#pragma warning disable 649
            public ulong VirtualAttributes;
#pragma warning restore 649
        }

        // run through memory https://www.codeproject.com/Articles/716227/Csharp-How-to-Scan-a-Process-Memory
        // VirtualQueryEx each chunk
        // If mem_basic_info.state == mem_mapped - then GetMappedFileName, if voron/buffer - sum for it if it is in mem using flag like in WillCauseHardPageFault

        [StructLayout(LayoutKind.Sequential)]
        public struct SYSTEM_INFO
        {
            public ushort processorArchitecture;

            // ReSharper disable once FieldCanBeMadeReadOnly.Local
            ushort reserved;

            public uint pageSize;
            public IntPtr minimumApplicationAddress;
            public IntPtr maximumApplicationAddress;
            public IntPtr activeProcessorMask;
            public uint numberOfProcessors;
            public uint processorType;
            public uint allocationGranularity;
            public ushort processorLevel;
            public ushort processorRevision;
        }

        public enum MemoryProtectionConstants : uint
        {
            // https://msdn.microsoft.com/en-us/library/windows/desktop/aa366786(v=vs.85).aspx
            PAGE_READWRITE = 0x04
        }

        public enum MemoryStateConstants : uint
        {
            // https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775(v=vs.85).aspx
            MEM_COMMIT = 0x1000,
            MEM_FREE = 0x10000,
            MEM_RESERVE = 0x2000
        }

        public enum MemoryTypeConstants : uint
        {
            // https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775(v=vs.85).aspx
            MEM_IMAGE = 0x1000000,
            MEM_MAPPED = 0x40000,
            MEM_PRIVATE = 0x20000
        }



        [DllImport("kernel32.dll")]
        public static extern void GetSystemInfo(out SYSTEM_INFO lpSystemInfo);

        private static readonly byte[][] RelevantFilesPostFixes =
        {
            Encoding.ASCII.GetBytes(".voron"),
            Encoding.ASCII.GetBytes(".buffers")
        };

        private static readonly UnmanagedBuffersPool BuffersPool = new UnmanagedBuffersPool("AddressWillCauseHardPageFault");

        private static uint? _pageSize;

        public static uint PageSize
        {
            get
            {
                if (_pageSize != null)
                    return _pageSize.Value;
                GetSystemInfo(out var systemInfo);
                _pageSize = systemInfo.pageSize;
                return _pageSize.Value;
            }
        }

        private static string GetEncodedFilename(IntPtr processHandle, ref Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryBasicInformation)
        {
            var memData = BuffersPool.Allocate(2048);
            var pFilename = memData.Address;
            try
            {
                int stringLength;
                stringLength = Win32MemoryProtectMethods.GetMappedFileName(processHandle, memoryBasicInformation.BaseAddress.ToPointer(), pFilename, 2048);

                if (stringLength == 0)
                    return null;

                var foundRelevantFilename = false;
                foreach (var item in RelevantFilesPostFixes)
                {
                    fixed (byte* pItem = item)
                    {
                        if (stringLength < item.Length ||
                            Memory.Compare(pItem, pFilename + stringLength - item.Length, item.Length) != 0)
                            continue;
                        foundRelevantFilename = true;
                        break;
                    }
                }
                if (foundRelevantFilename == false)
                    return null;
                return Encodings.Utf8.GetString(pFilename, stringLength);
            }
            finally
            {
                BuffersPool.Return(memData);
            }
        }

        public static (long ProcessClean, DynamicJsonArray Json) GetMaps()
        {
            long processClean = 0;

            const uint uintMaxVal = uint.MaxValue;
            var dja = new DynamicJsonArray();

            GetSystemInfo(out var systemInfo);

            var procMinAddress = systemInfo.minimumApplicationAddress;
            var procMaxAddress = systemInfo.maximumApplicationAddress;
            var processHandle = GetCurrentProcess();
            var results = new Dictionary<string, (long Size, long Clean, long Dirty)>();

            while (procMinAddress.ToInt64() < procMaxAddress.ToInt64())
            {
                Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryBasicInformation;
                Win32MemoryProtectMethods.VirtualQueryEx(processHandle, (byte*)procMinAddress.ToPointer(),
                    &memoryBasicInformation, new UIntPtr((uint)sizeof(Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION)));

                // if this memory chunk is accessible
                if (memoryBasicInformation.Protect == (uint)MemoryProtectionConstants.PAGE_READWRITE &&
                    memoryBasicInformation.State == (uint)MemoryStateConstants.MEM_COMMIT &&
                    memoryBasicInformation.Type == (uint)MemoryTypeConstants.MEM_MAPPED)
                {
                    var encodedString = GetEncodedFilename(processHandle, ref memoryBasicInformation);
                    if (encodedString != null)
                    {
                        var regionSize = memoryBasicInformation.RegionSize.ToInt64();
                        for (long size = uintMaxVal; size < regionSize + uintMaxVal; size += uintMaxVal)
                        {
                            var partLength = size > regionSize ? regionSize % uintMaxVal : uintMaxVal;

                            var totalDirty = AddressWillCauseHardPageFault((byte*)memoryBasicInformation.BaseAddress.ToPointer(), (uint)partLength,
                                performCount: true);
                            var totalClean = partLength - totalDirty;

                            if (results.TryGetValue(encodedString, out var values))
                            {
                                var prevValClean = values.Item1 + totalClean;
                                var prevValDirty = values.Item2 + totalDirty;
                                var prevValSize = values.Item3 + partLength;
                                results[encodedString] = (prevValSize, prevValClean, prevValDirty);
                            }
                            else
                            {
                                results[encodedString] = (partLength, totalClean, totalDirty);
                            }

                            processClean += totalClean;

                        }
                    }
                }

                // move to the next memory chunk
                procMinAddress = new IntPtr(procMinAddress.ToInt64() + memoryBasicInformation.RegionSize.ToInt64());
            }

            foreach (var result in results)
            {
                var djv = new DynamicJsonValue
                {
                    ["File"] = result.Key,
                    ["Size"] = result.Value.Size,
                    ["SizeHumanly"] = Sizes.Humane(result.Value.Size),
                    ["Rss"] = "N/A",
                    ["SharedClean"] = "N/A",
                    ["SharedDirty"] = "N/A",
                    ["PrivateClean"] = "N/A",
                    ["PrivateDirty"] = "N/A",
                    ["TotalClean"] = result.Value.Clean,
                    ["TotalCleanHumanly"] = Sizes.Humane(result.Value.Clean),
                    ["TotalDirty"] = result.Value.Dirty,
                    ["TotalDirtyHumanly"] = Sizes.Humane(result.Value.Dirty)
                };

                dja.Add(djv);
            }

            return (processClean, dja);
        }

        public static bool WillCauseHardPageFault(byte* address, long length)
        {
            if (length > int.MaxValue)
                return true; // truelly big sizes are not going to be handled

            return AddressWillCauseHardPageFault(address, length) > 0;
        }

        public static uint AddressWillCauseHardPageFault(byte* address, long length, bool performCount = false)
        {
            uint count = 0;
            var remain = length % PageSize == 0 ? 0 : 1;
            var pages = (length / PageSize) + remain;
            AllocatedMemoryData memData = null;

            PPSAPI_WORKING_SET_EX_INFORMATION* pWsInfo;
            var p = stackalloc PPSAPI_WORKING_SET_EX_INFORMATION[2];
            if (pages > 2)
            {
                memData = BuffersPool.Allocate((int)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages));
                var wsInfo = new IntPtr(memData.Address);
                pWsInfo = (PPSAPI_WORKING_SET_EX_INFORMATION*)wsInfo.ToPointer();
            }
            else
            {
                pWsInfo = p;
            }

            try
            {
                for (var i = 0; i < pages; i++)
                    pWsInfo[i].VirtualAddress = address + (i * PageSize);

                if (QueryWorkingSetEx(GetCurrentProcess(), (byte*)pWsInfo, (uint)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages)) == false)
                    throw new MemoryInfoException(
                        $"Failed to QueryWorkingSetEx address: {new IntPtr(address).ToInt64()}, with length: {length}. processId = {GetCurrentProcess()}");

                for (int i = 0; i < pages; i++)
                {
                    var flag = pWsInfo[i].VirtualAttributes & 0x00000001;
                    if (flag == 0)
                    {
                        if (performCount == false)
                            return 1;
                        count += PageSize;
                    }

                }
                return count;

            }
            finally
            {
                if (memData != null)
                {
                    BuffersPool.Return(memData);
                }
            }
        }
    }
}
