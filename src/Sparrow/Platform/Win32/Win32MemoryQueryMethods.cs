using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Exceptions;
using Sparrow.Global;
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

        private static readonly byte[] VoronPostFix = new Byte[] {(byte)'.', (byte)'v', (byte)'o', (byte)'r', (byte)'o', (byte)'n'};
        private static readonly byte[] BufferPostFix = new Byte[] { (byte)'.', (byte)'b', (byte)'u', (byte)'f', (byte)'f', (byte)'e', (byte)'r', (byte)'s' };

        public static DynamicJsonArray GetMaps()
        {
            var dja = new DynamicJsonArray();

            SYSTEM_INFO systemInfo;
            GetSystemInfo(out systemInfo);

            var procMinAddress = systemInfo.minimumApplicationAddress;
            var procMaxAddress = systemInfo.maximumApplicationAddress;
            var processHandle = GetCurrentProcess();

            var bytesRead = new UIntPtr(0); // number of bytes read with ReadProcessMemory
            var results = new Dictionary<string, Tuple<long, long>>();
            var bufferArr = new byte[64 * Constants.Size.Megabyte];
            var filenameString = new byte[2048];


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
                    var regionSize = memoryBasicInformation.RegionSize.ToInt64();
                    long offset = 0;
                    for (long size = 0; size < regionSize + 64 * Constants.Size.Megabyte; size += 64 * Constants.Size.Megabyte)
                    {
                        var partLength = size <= regionSize ? 64 * Constants.Size.Megabyte : size - regionSize;
                        fixed (byte* buffer = bufferArr)
                            ReadProcessMemory(processHandle,
                                (byte*)memoryBasicInformation.BaseAddress.ToPointer() + offset, buffer, (uint)partLength, bytesRead);
                        offset += partLength;

                        var totalDirty = AddrWillCauseHardPageFault((byte*)memoryBasicInformation.BaseAddress.ToPointer(), (uint)partLength,
                            performCount: true);
                        var totalClean = partLength - totalDirty;
                        int stringLength;
                        fixed (byte* pFilename = filenameString)
                            stringLength = GetMappedFileName(processHandle, memoryBasicInformation.BaseAddress.ToPointer(), pFilename, 2048);

                        if (stringLength == 0)
                            break;

                        var posInBuffersPostFix = BufferPostFix.Length - 1;
                        var posInVoronPostFix = VoronPostFix.Length - 1;
                        bool matchBufferPostFix = false;
                        bool matchVoronPostFix = false;
                        for (int i = stringLength - 1; i >= 0; i--)
                        {
                            if (posInBuffersPostFix >= 0 && (i == stringLength - 1 || matchBufferPostFix))
                                matchBufferPostFix = BufferPostFix[posInBuffersPostFix] == filenameString[i];
                            posInBuffersPostFix--;

                            if (posInVoronPostFix >= 0 && (i == stringLength - 1 || matchVoronPostFix))
                                matchVoronPostFix = VoronPostFix[posInVoronPostFix] == filenameString[i];
                            posInVoronPostFix--;
                        }

                        if (matchBufferPostFix == false &&
                            matchVoronPostFix == false)
                            break;

                        var encodedString = Encodings.Utf8.GetString(filenameString, 0, stringLength);
                        if (results.ContainsKey(encodedString))
                        {
                            var prevValClean = results[encodedString].Item1 + totalClean;
                            var prevValDirty = results[encodedString].Item2 + totalDirty;
                            results[encodedString] = new Tuple<long, long>(prevValClean, prevValDirty);
                        }
                        else
                        {
                            results[encodedString] = new Tuple<long, long>(totalClean, totalDirty);
                        }
                    }
                }

                // move to the next memory chunk
                procMinAddress = new IntPtr(procMinAddress.ToInt64() + memoryBasicInformation.RegionSize.ToInt64());
            }

            foreach (var result in results)
            {
                var clean = result.Value.Item1;
                var dirty = result.Value.Item2;
                var djaInner = new DynamicJsonArray();
                var djvInner = new DynamicJsonValue
                {
                    ["TotalClean"] = clean,
                    ["TotalCleanHumanly"] = Sizes.Humane(clean),
                    ["TotalDirty"] = dirty,
                    ["TotalDirtyHumanly"] = Sizes.Humane(dirty)
                };
                djaInner.Add(djvInner);

                dja.Add(new DynamicJsonValue
                {
                    [result.Key] = djaInner
                });
            }
            return dja;
        }

        public static bool WillCauseHardPageFault(byte* addr, long length)
        {
            if (length > int.MaxValue)
                return true; // truelly big sizes are not going to be handled

            return AddrWillCauseHardPageFault(addr, length) > 0;
        }

        public static int AddrWillCauseHardPageFault(byte*addr, long length, bool performCount = false)
        {
            const int pagesize = 64 * Constants.Size.Kilobyte;
            int count = 0;
            var remain = length % pagesize == 0 ? 0 : 1;
            var pages = (length / pagesize) + remain;

            IntPtr wsInfo = IntPtr.Zero;
            PPSAPI_WORKING_SET_EX_INFORMATION* pWsInfo;
            var p = stackalloc PPSAPI_WORKING_SET_EX_INFORMATION[2];
            if (pages > 2)
            {
                wsInfo = Marshal.AllocHGlobal((int)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages));
                pWsInfo = (PPSAPI_WORKING_SET_EX_INFORMATION*)wsInfo.ToPointer();
            }
            else
            {
                pWsInfo = p;
            }

            try
            {
                for (var i = 0; i < pages; i++)
                    pWsInfo[i].VirtualAddress = addr + (i * pagesize);

                if (QueryWorkingSetEx(GetCurrentProcess(), (byte*)pWsInfo, (uint)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages)) == false)
                    throw new MemoryInfoException($"Failed to QueryWorkingSetEx addr: {new IntPtr(addr).ToInt64()}, with length: {length}. processId = {GetCurrentProcess()}");

                for (int i = 0; i < pages; i++)
                {
                    var flag = pWsInfo[i].VirtualAttributes & 0x00000001;
                    if (flag == 0)
                    {
                        if (performCount == false)
                            return 1;
                        count += pagesize;
                    }

                }
                return count;

            }
            finally
            {
                if (wsInfo != IntPtr.Zero)
                    Marshal.FreeHGlobal(wsInfo);
            }
        }
    }
}
