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

    [StructLayout(LayoutKind.Explicit)]

        unsafe public struct set_sched_param
        {
            [FieldOffset(0)]
            public int sched_priority;
            [FieldOffset(4)]
            public int sched_curpriority;
            [FieldOffset(8)]
            fixed int reserved[8];
            [FieldOffset(8)]
            // This is for the struct __ss (http://www.qnx.com/developers/docs/6.4.0/neutrino/lib_ref/s/sched_param.html)
            fixed int __ss_un[10];
    	}

	unsafe public struct sched_param
	{
	    public int sched_priority;
	}
}
