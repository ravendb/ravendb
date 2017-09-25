using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public static class DiskSpaceChecker
    {
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
            out ulong lpFreeBytesAvailable,
            out ulong lpTotalNumberOfBytes,
            out ulong lpTotalNumberOfFreeBytes);

        public static DiskSpaceResult GetFreeDiskSpace(string pathToCheck, DriveInfo[] driveInfo)
        {
            if (string.IsNullOrEmpty(pathToCheck))
                return null;

            if (Path.IsPathRooted(pathToCheck) && pathToCheck.StartsWith("\\\\") == false)
            {
                var root = Path.GetPathRoot(pathToCheck);

                foreach (var drive in driveInfo)
                {
                    if (root.Contains(drive.Name) == false)
                        continue;

                    return new DiskSpaceResult
                    {
                        DriveName = root,
                        TotalFreeSpace = new Size(drive.TotalFreeSpace, SizeUnit.Bytes),
                        TotalSize = new Size(drive.TotalSize, SizeUnit.Bytes)
                    };
                }

                return null;
            }

            if (pathToCheck.StartsWith("\\\\"))
            {
                var uncRoot = Path.GetPathRoot(pathToCheck);

                if (PlatformDetails.RunningOnPosix) // TODO
                    return null;

                var success = GetDiskFreeSpaceEx(uncRoot, out ulong freeBytesAvailable, out ulong totalNumberOfBytes, out _);

                if (success == false)
                    return null;

                return new DiskSpaceResult
                {
                    DriveName = uncRoot,
                    TotalFreeSpace = new Size((long)freeBytesAvailable, SizeUnit.Bytes),
                    TotalSize = new Size((long)totalNumberOfBytes, SizeUnit.Bytes)
                };
            }

            return null;
        }

        public class DiskSpaceResult
        {
            public string DriveName { get; set; }

            public Size TotalFreeSpace { get; set; }

            public Size TotalSize { get; set; }
        }
    }
}
