using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

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

        public static DiskSpaceResult GetFreeDiskSpace(string pathToCheck, DriveInfo[] drivesInfo)
        {
            if (string.IsNullOrEmpty(pathToCheck))
                return null;

            if (PlatformDetails.RunningOnPosix)
            {
                var statvfs = default(Statvfs);
                if (Syscall.statvfs(pathToCheck, ref statvfs) != 0)
                    return null;

                return new DiskSpaceResult
                {
                    DriveName = Syscall.GetRootMountString(drivesInfo, pathToCheck),
                    TotalFreeSpace = new Size((long)(statvfs.f_bsize * statvfs.f_bavail), SizeUnit.Bytes),
                    TotalSize = new Size((long)(statvfs.f_bsize * statvfs.f_blocks), SizeUnit.Bytes)
                };
            }

            if (Path.IsPathRooted(pathToCheck) && pathToCheck.StartsWith("\\\\") == false)
            {
                var root = Path.GetPathRoot(pathToCheck);

                foreach (var drive in drivesInfo)
                {
                    if (root.IndexOf(drive.Name, StringComparison.OrdinalIgnoreCase) < 0)
                        continue;

                    return new DiskSpaceResult
                    {
                        DriveName = root,
                        VolumeLabel = drive.VolumeLabel,
                        TotalFreeSpace = new Size(drive.TotalFreeSpace, SizeUnit.Bytes),
                        TotalSize = new Size(drive.TotalSize, SizeUnit.Bytes)
                    };
                }

                return null;
            }

            if (pathToCheck.StartsWith("\\\\"))
            {
                var uncRoot = Path.GetPathRoot(pathToCheck);

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
    }

    public class DiskSpaceResult
    {
        public string DriveName { get; set; }

        public string VolumeLabel { get; set; }

        public Size TotalFreeSpace { get; set; }

        public Size TotalSize { get; set; }
    }
}
