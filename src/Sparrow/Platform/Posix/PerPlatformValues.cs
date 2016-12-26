using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    public class PerPlatformValues
    {
        public class SyscallNumbers
        {
            public static long SYS_gettid =
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 224
                : 186;
        }

        public class OpenFlags
        {
            public static Posix.OpenFlags O_DIRECT = (Posix.OpenFlags) (
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 65536 // value directly from printf("%d", O_DIRECT) on the pi
                : 16384); // value directly from printf("%d", O_DIRECT)

            public static Posix.OpenFlags O_DIRECTORY = (Posix.OpenFlags) (
            (RuntimeInformation.OSArchitecture == Architecture.Arm ||
             RuntimeInformation.OSArchitecture == Architecture.Arm64)
                ? 16384 // value directly from printf("%d", O_DIRECTORY)
                : 65536); // value directly from printf("%d", O_DIRECTORY) on the pi
        }
    }
}