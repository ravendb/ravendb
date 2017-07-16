using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Exceptions;

namespace Sparrow.Platform.Posix
{
    public static unsafe class PosixMemoryQueryMethods
    {
        public static bool WillCauseHardPageFault(IntPtr addr, long length)
        {
            Debug.Assert(addr.ToInt64() % Syscall.PageSize == 0);

            var vecSize = (length + Syscall.PageSize - 1) / Syscall.PageSize;
            var vec = Marshal.AllocHGlobal((int)vecSize);
            if (Syscall.mincore(addr.ToPointer(), new IntPtr(length), (char*)vec.ToPointer()) != 0)
                throw new MemoryInfoException($"Failed to mincore addr: {addr.ToInt64()}, with length: {length}. Last Error = {Marshal.GetLastWin32Error()}");

            for (var i = 0; i < vecSize; i++)
            {
                if ((*((byte*)vec.ToPointer() + i) & 1) == 0)
                    continue;

                return false;
            }
            return true;
        }
    }
}
