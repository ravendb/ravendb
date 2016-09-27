using System;
using System.Runtime.InteropServices;
using System.Security;

namespace Sparrow
{
    public static unsafe class Win32UnmanagedMemory
    {
        [DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = false)]
        [SecurityCritical]
        public static extern IntPtr Copy(byte* dest, byte* src, long count);

        [DllImport("msvcrt.dll", EntryPoint = "memcmp", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern int Compare(byte* b1, byte* b2, long count);

        [DllImport("msvcrt.dll", EntryPoint = "memmove", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern int Move(byte* dest, byte* src, long count);

        [DllImport("msvcrt.dll", EntryPoint = "memset", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        public static extern IntPtr Set(byte* dest, int c, long count);
    }
}
