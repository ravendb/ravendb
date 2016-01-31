using System.Runtime.InteropServices;

namespace Sparrow.Platform
{
    public static class Platform
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
    }
}