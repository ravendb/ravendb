using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Exceptions;

namespace Sparrow.Server.Platform.Posix
{
    public static unsafe class PosixMemoryQueryMethods
    {
        public static bool WillCauseHardPageFault(byte* address, long length)
        {
            if (length > int.MaxValue)
                return true; // truelly big sizes are not going to be handled

            Debug.Assert(new IntPtr(address).ToInt64() % Syscall.PageSize == 0);

            var vecSize = (int)((length + Syscall.PageSize - 1) / Syscall.PageSize);

            var p = stackalloc IntPtr[2];
            IntPtr vec = IntPtr.Zero;
            char* pVec;
            if (vecSize > 4)
            {
                vec = Marshal.AllocHGlobal(vecSize);
                pVec = (char *)vec.ToPointer();
            }
            else
            {
                pVec = (char*)p;
            }

            try
            {
                if (Syscall.mincore(address, new IntPtr(length), pVec) != 0)
                    throw new MemoryInfoException($"Failed to mincore address: {new IntPtr(address).ToInt64()}, with length: {length}. Last Error = {Marshal.GetLastWin32Error()}");

                for (var i = 0; i < vecSize; i++)
                {
                    if ((*((byte*)pVec + i) & 1) == 0)
                        continue;

                    return true;
                }
                return false;
            }
            finally
            {
                if (vec != IntPtr.Zero)
                    Marshal.FreeHGlobal(vec);
            }
        }
    }
}
