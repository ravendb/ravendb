using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow
{
    public static unsafe class UnmanagedMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IntPtr Copy(byte* dest, byte* src, long count)
        {
            Debug.Assert(count >= 0);
            return PlatformDetails.RunningOnPosix
                ? Syscall.Copy(dest, src, count)
                : Win32UnmanagedMemory.Copy(dest, src, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(byte* b1, byte* b2, long count)
        {
            Debug.Assert(count >= 0);
            return PlatformDetails.RunningOnPosix
                ? Syscall.Compare(b1, b2, count)
                : Win32UnmanagedMemory.Compare(b1, b2, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Move(byte* dest, byte* src, long count)
        {
            Debug.Assert(count >= 0);
            return PlatformDetails.RunningOnPosix
                ? Syscall.Move(dest, src, count)
                : Win32UnmanagedMemory.Move(dest, src, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IntPtr Set(byte* dest, int c, long count)
        {
            Debug.Assert(count >= 0);
            return PlatformDetails.RunningOnPosix
                ? Syscall.Set(dest, c, count)
                : Win32UnmanagedMemory.Set(dest, c, count);
        }
    }
}
