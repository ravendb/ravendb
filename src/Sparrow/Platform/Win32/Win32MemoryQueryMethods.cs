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


        public static bool WillCauseHardPageFault(byte* addr, long length)
        {
            if (length > int.MaxValue)
                return true; // truelly big sizes are not going to be handled

            const int pagesize = 64 * Constants.Size.Kilobyte;

            var pages = length / pagesize;

            IntPtr wsInfo = IntPtr.Zero;
            PPSAPI_WORKING_SET_EX_INFORMATION* pWsInfo;
            var p = stackalloc PPSAPI_WORKING_SET_EX_INFORMATION[2];
            if (pages > 2)
            {
                wsInfo = Marshal.AllocHGlobal((int)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION)*pages));
                pWsInfo = (PPSAPI_WORKING_SET_EX_INFORMATION *)wsInfo.ToPointer();
            }
            else
            {
                pWsInfo = p;
            }

            try
            {
                for (var i = 0; i < pages; i++)
                    pWsInfo[i].VirtualAddress = addr + (i * pagesize);

                if (QueryWorkingSetEx(GetCurrentProcess(), (byte *)pWsInfo, (uint)(sizeof(PPSAPI_WORKING_SET_EX_INFORMATION) * pages)) == false)
                    throw new MemoryInfoException($"Failed to QueryWorkingSetEx addr: {new IntPtr(addr).ToInt64()}, with length: {length}. processId = {GetCurrentProcess()}");

                for (int i = 0; i < pages; i++)
                {
                    var flag = pWsInfo[i].VirtualAttributes & 0x00000001;
                    if (flag == 0)
                        return true;
                }
                return false;

            }
            finally 
            {
                if (wsInfo != IntPtr.Zero)
                    Marshal.FreeHGlobal(wsInfo);
            }
        }
    }
}
