using System;
using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix.macOS
{
    public static unsafe class macSyscall
    {
        internal const string Pthread = "pthread";

        [DllImport(Syscall.LIBC_6, SetLastError = true)]
        public static extern int sysctl(int[] name, uint nameLen, void* oldP, int* oldLenP, void* newP, UIntPtr newLen);

        [DllImport(Syscall.LIBC_6, SetLastError = true)]
        public static extern int mach_host_self();

        [DllImport(Syscall.LIBC_6, SetLastError = true)]
        public static extern int host_page_size(int machHost, uint* pageSize);

        [DllImport(Syscall.LIBC_6, SetLastError = true)]
        public static extern int host_statistics64(int machHost, int flavor, void* hostInfoT, int* hostInfoCount);

        [DllImport(Syscall.LIBC_6, SetLastError = true)]
        public static extern int proc_pidinfo(int pid, int flavor, ulong arg, void* buffer, int bufferSize);

        [DllImport(Pthread, CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern ulong pthread_self();
    }
}
