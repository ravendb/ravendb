using System;
using System.Runtime.InteropServices;
using Sparrow.Exceptions;
using Sparrow.Global;

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


        public static bool WillCauseHardPageFault(IntPtr addr, long length)
        {
            const int pagesize = 64 * Constants.Size.Kilobyte;

            var pages = length / pagesize;
            var wsInfo = new PPSAPI_WORKING_SET_EX_INFORMATION[pages];
            for (var i = 0; i < pages; i++)
                wsInfo[i].VirtualAddress = (byte*)addr.ToPointer() + (i * pagesize);

            fixed (void* pWsInfo = wsInfo)
            {
                if (QueryWorkingSetEx(GetCurrentProcess(), (byte*)pWsInfo, (uint)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages)) == false)
                    throw new MemoryInfoException($"Failed to QueryWorkingSetEx addr: {addr.ToInt64()}, with length: {length}. processId = {GetCurrentProcess()}");

            }

            for (int i = 0; i < pages; i++)
            {
                var flag = wsInfo[i].VirtualAttributes & 0x00000001;
                if (flag == 0)
                    return false;
            }
            return true;
        }
    }
}
