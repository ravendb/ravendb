using System;
using System.Runtime.InteropServices;

namespace Sparrow
{
    public static class PosixThreadsMethods
    {
        private const string LIBC_6 = "libc.so.6";

        [DllImport(LIBC_6, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = true)]
        public static unsafe extern int pthread_setschedparam(ulong thread, int policy, sched_param* param);

        [DllImport(LIBC_6, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = true)]
        public static unsafe extern int pthread_getschedparam(ulong thread, int* policy, sched_param* param);

        [DllImport(LIBC_6, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern ulong pthread_self();
    }

    public struct sched_param
    {
        public int sched_priority;
    }
}
