using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Platform;

namespace Tests.Infrastructure.InterversionTest
{
    public class ServerBuildDownloadInfo
    {
        public string Platform {get;set;}

        public string Version {get;set;}

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
            return new ServerBuildDownloadInfo()
            {
                Version = version,
                Platform = GetPlatformString(version)
            };
        }
        
        private static string GetPlatformString(string version)
        {
            if (PlatformDetails.RunningOnLinux)
            {
                return RuntimeInformation.ProcessArchitecture switch
                {
                    Architecture.X64 => "linux-x64",
                    Architecture.Arm64 => "linux-arm64",
                    // we don't have a x86 linux build. All we got is RPi/ARM build.
                    Architecture.Arm => "raspberry-pi",
                    _ => throw new PlatformNotSupportedException($"There is no RavenDB {version} server package for Linux {RuntimeInformation.ProcessArchitecture}.")
                };
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                if (version.StartsWith("4.1")
                    || version.StartsWith("4.0")) {
                    return "osx-x64";
                }
                
                return "macos-x64";
            }

            if (PlatformDetails.Is32Bits)
            {
                return "windows-x86";
            }

            return "windows-x64";
        }
    }
}
