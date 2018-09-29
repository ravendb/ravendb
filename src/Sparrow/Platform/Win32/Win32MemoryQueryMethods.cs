using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using static Sparrow.Platform.Win32.Win32MemoryProtectMethods;
// ReSharper disable InconsistentNaming

namespace Sparrow.Platform.Win32
{
    public static unsafe class Win32MemoryQueryMethods
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetCurrentProcess();

        [DllImport("psapi.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool QueryWorkingSetEx(IntPtr hProcess, byte* pv, uint cb);

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

        private static string GetEncodedFilename(IntPtr processHandle, ref MEMORY_BASIC_INFORMATION memoryBasicInformation)
        {
            var memData = BuffersPool.Allocate(2048);
            var pFilename = memData.Address;
            try
            {
                int stringLength;
                stringLength = GetMappedFileName(processHandle, memoryBasicInformation.BaseAddress.ToPointer(), pFilename, 2048);

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
                MEMORY_BASIC_INFORMATION memoryBasicInformation;
                VirtualQueryEx(processHandle, (byte*)procMinAddress.ToPointer(),
                    &memoryBasicInformation, new UIntPtr((uint)sizeof(MEMORY_BASIC_INFORMATION)));

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
                                var prevValClean = values.Clean + totalClean;
                                var prevValDirty = values.Dirty + totalDirty;
                                var prevValSize = values.Size + partLength;
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
                    throw new MemoryInfoException($"Failed to QueryWorkingSetEx address: {new IntPtr(address).ToInt64()}, with length: {length}. processId = {GetCurrentProcess()}");

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
