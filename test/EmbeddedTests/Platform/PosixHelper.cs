using System.IO;
using System.Runtime.InteropServices;

namespace EmbeddedTests.Platform
{
    public static class PosixHelper
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public static string FixLinuxPath(string path)
        {
            if (path != null)
            {
                var length = Path.GetPathRoot(path).Length;
                if (length > 0)
                    path = "/" + path.Substring(length);
                path = path.Replace('\\', '/');
                path = path.Replace("/./", "/");
                path = path.Replace("//", "/");
            }

            return path;
        }
    }
}
