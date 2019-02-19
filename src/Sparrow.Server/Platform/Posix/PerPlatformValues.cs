using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    public class PerPlatformValues
    {
        public class SyscallNumbers
        {
            public static long SYS_gettid =
            (RuntimeInformation.OSArchitecture == Architecture.Arm)
                ? 224
                : (RuntimeInformation.OSArchitecture == Architecture.Arm64)
                    ? 178
                    : 186;
        }

        public class OpenFlags
        {
            public static Posix.OpenFlags O_DIRECT = (Posix.OpenFlags) (
                PlatformDetails.RunningOnMacOsx ? 0 : // O_DIRECT is not supported in MacOsx, we use fncnlt(F_NOCACHE) instead
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 65536 // value directly from printf("%d", O_DIRECT) on the pi
                : 16384); // value directly from printf("%d", O_DIRECT)

            public static Posix.OpenFlags O_DIRECTORY = (Posix.OpenFlags) (
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 16384 // value directly from printf("%d", O_DIRECTORY)
                : 65536); // value directly from printf("%d", O_DIRECTORY) on the pi

            public static Posix.OpenFlags O_LARGEFILE = (Posix.OpenFlags)(
                PlatformDetails.RunningOnMacOsx ? 0 : // O_LARGEFILE is supported by default in MacOsx and this flag doesn't exists in mac
           (RuntimeInformation.OSArchitecture == Architecture.Arm ||
            RuntimeInformation.OSArchitecture == Architecture.Arm64)
               ? 131072 // value directly from printf("%d", O_DIRECT) on the pi
               : 32768); // value directly from printf("%d", O_DIRECT)

            public static Posix.OpenFlags O_CREAT = (Posix.OpenFlags)(
                PlatformDetails.RunningOnMacOsx
                    ? 0x00000200
                    : 0x00000040);

            public static Posix.OpenFlags O_EXCL = (Posix.OpenFlags)(
                PlatformDetails.RunningOnMacOsx
                    ? 0x00000800
                    : 0x00000080);

            public static Posix.OpenFlags O_TRUNC = (Posix.OpenFlags)(
                PlatformDetails.RunningOnMacOsx
                    ? 0x00000400
                    : 0x00000200);

            public static Posix.OpenFlags O_DSYNC = (Posix.OpenFlags)(
                PlatformDetails.RunningOnMacOsx
                    ? 0x00400000
                    : 4096);            
        }

        public class SysconfNames
        {
            public static int _SC_PAGESIZE =
                PlatformDetails.RunningOnMacOsx
                    ? 0x1d
                    : 0x1e;
        }
    }
}
