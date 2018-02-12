using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    public static class OsxSyscall
    {
        internal const string Pthread = "pthread";

        [DllImport(Pthread, CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern ulong pthread_self();
    }
}
