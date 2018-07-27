using System.IO;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ServerBuildDownloadInfo
    {
        public string Platform { get; set; }

        public string Version { get; set; }

        public string PackageName => $"RavenDB-{Version}-{Platform}";

        public string PackageDownloadFileName => $"{PackageFileName}.dl";

        public string PackageFileName
        {
            get
            {
                string archiveExtension = Platform.StartsWith("windows")
                    ? "zip"
                    : "tar.bz2";

                return $"{PackageName}.{archiveExtension}";
            }
        }

        public string GetServerDirectory(string serverDownloadPath)
        {
            return Path.Combine(serverDownloadPath, PackageName);
        }

        public static ServerBuildDownloadInfo Create(string version)
        {
            return new ServerBuildDownloadInfo
            {
                Version = version,
                Platform = GetPlatformString()
            };
        }

        private static string GetPlatformString()
        {
            if (PlatformDetails.RunningOnLinux
                && PlatformDetails.Is32Bits == false)
            {
                return "linux-x64";
            }

            if (PlatformDetails.RunningOnLinux && PlatformDetails.Is32Bits)
            {
                // we don't have a x86 linux build. All we got is RPi/ARM build.
                return "raspberry-pi";
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                return "osx-x64";
            }

            if (PlatformDetails.Is32Bits)
            {
                return "windows-x86";
            }

            return "windows-x64";
        }
    }
}
