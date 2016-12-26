using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    public class OpenFlagsThatAreDifferentBetweenPlatforms
    {
        public static OpenFlags O_DIRECT =(OpenFlags)(
        (RuntimeInformation.OSArchitecture == Architecture.Arm ||
         RuntimeInformation.OSArchitecture == Architecture.Arm64)
            ? 65536 // value directly from printf("%d", O_DIRECT) on the pi
            : 16384); // value directly from printf("%d", O_DIRECT)

        public static OpenFlags O_DIRECTORY = (OpenFlags) (
        (RuntimeInformation.OSArchitecture == Architecture.Arm ||
         RuntimeInformation.OSArchitecture == Architecture.Arm64)
            ? 16384 // value directly from printf("%d", O_DIRECTORY)
            : 65536); // value directly from printf("%d", O_DIRECTORY) on the pi

    }
}