using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow.Utils
{
    public static class DiskSpaceChecker
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(DiskSpaceChecker).FullName);

        // from https://github.com/dotnet/corefx/blob/9c06da6a34fcefa6fb37776ac57b80730e37387c/src/Common/src/System/IO/PathInternal.Windows.cs#L52
        public const short WindowsMaxPath = short.MaxValue;

        public const int LinuxMaxPath = 4096;

        public static DiskSpaceResult GetDiskSpaceInfo(string pathToCheck, DriveInfoBase driveInfoBase = null)
        {
            if (string.IsNullOrEmpty(pathToCheck))
                return null;

            if (PlatformDetails.RunningOnPosix)
            {
                var statvfs = default(Statvfs);
                var result = Syscall.statvfs(pathToCheck, ref statvfs);
                if (result != 0)
                {
                    var error = Syscall.ErrorNumberToStatusCode(result) ?? result.ToString();
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to get file system statistics for path: {pathToCheck}, error: {error}");

                    return null;
                }

                return new DiskSpaceResult
                {
                    DriveName = driveInfoBase?.DriveName,
                    TotalFreeSpace = new Size((long)(statvfs.f_bsize * statvfs.f_bavail), SizeUnit.Bytes),
                    TotalSize = new Size((long)(statvfs.f_bsize * statvfs.f_blocks), SizeUnit.Bytes)
                };
            }
            
            var success = GetDiskFreeSpaceEx(pathToCheck, out var freeBytesAvailable, out var totalNumberOfBytes, out _);
            if (success == false)
            {
                if (Logger.IsInfoEnabled)
                {
                    var error = Marshal.GetLastWin32Error();
                    Logger.Info($"Failed to get the free disk space, path: {pathToCheck}, error: {error}");
                }
                return null;
            }

            var driveName = driveInfoBase?.DriveName;
            return new DiskSpaceResult
            {
                DriveName = driveName,
                VolumeLabel = GetVolumeLabel(driveName),
                TotalFreeSpace = new Size((long)freeBytesAvailable, SizeUnit.Bytes),
                TotalSize = new Size((long)totalNumberOfBytes, SizeUnit.Bytes)
            };
        }

        private static string GetVolumeLabel(string path)
        {
            if (path == null)
                return null;

            if (path.StartsWith("\\", StringComparison.OrdinalIgnoreCase))
            {
                // network path
                return null;
            }

            try
            {
                var driveInfo = new DriveInfo(path);
                return driveInfo.VolumeLabel;
            }
            catch
            {
                return null;
            }
        }

        public static DriveInfoBase GetDriveInfo(string path, DriveInfo[] drivesInfo)
        {
            var driveName = GetDriveName(path, drivesInfo);

            return new DriveInfoBase
            {
                DriveName = driveName
            };
        }

        private static string GetDriveName(string path, DriveInfo[] drivesInfo)
        {
            try
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
                    var windowsRealPath = GetWindowsRealPathByPath(path);
                    return Path.GetPathRoot(windowsRealPath);
                }

                var posixRealPath = GetPosixRealPath(path);
                return GetRootMountString(drivesInfo, posixRealPath) ?? posixRealPath;
            }
            catch (Exception e)
            {
                // failing here will prevent us from starting the storage environment
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to get the real path for: {path}", e);

                return path;
            }
        }

        private static unsafe string GetPosixRealPath(string path)
        {
            var byteArray = new byte[LinuxMaxPath];
            fixed (byte* buffer = byteArray)
            {
                var result = Syscall.readlink(path, buffer, LinuxMaxPath);
                if (result == -1)
                {
                    // not a symbolic link
                    return path;
                }

                var realPath = Encoding.UTF8.GetString(byteArray, 0, result);
                return realPath;
            }
        }

        private static string GetRootMountString(DriveInfo[] drivesInfo, string filePath)
        {
            string root = null;
            var matchSize = 0;

            foreach (var driveInfo in drivesInfo)
            {
                var mountNameSize = driveInfo.Name.Length;
                if (filePath.StartsWith(driveInfo.Name) == false)
                    continue;

                if (matchSize >= mountNameSize)
                    continue;

                matchSize = mountNameSize;
                root = driveInfo.Name;
            }

            return root;
        }

        private static string GetWindowsRealPathByPath(string path)
        {
            var handle = CreateFile(path,
                FILE_READ_EA,
                FileShare.Read,
                IntPtr.Zero,
                FileMode.Open,
                FILE_FLAG_BACKUP_SEMANTICS,
                IntPtr.Zero);

            if (handle == INVALID_HANDLE_VALUE)
            {
                if (Logger.IsInfoEnabled)
                {
                    var error = Marshal.GetLastWin32Error();
                    Logger.Info($"Failed to the handle for path: {path}, error: {error}");
                }

                return path;
            }

            try
            {
                return GetWindowsRealPathByHandle(handle) ?? path;
            }
            finally
            {
                CloseHandle(handle);
            }
        }

        public static unsafe string GetWindowsRealPathByHandle(IntPtr handle)
        {
            bool GetPath(uint bufferSize, out string outputPath)
            {
                var charArray = new char[bufferSize];
                fixed (char* buffer = charArray)
                {
                    var result = GetFinalPathNameByHandle(handle, buffer, bufferSize, 0);
                    if (result == 0)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            var error = Marshal.GetLastWin32Error();
                            Logger.Info($"Failed to get the final path name by handle, error: {error}");
                        }

                        outputPath = null;
                        return false;
                    }

                    // return value is the size of the path
                    if (result > bufferSize)
                    {
                        outputPath = null;
                        return false;
                    }

                    outputPath = new string(charArray, 0, (int)result);
                    return true;
                }
            }

            // this is called for each storage environment for the Data, Journals and Temp paths
            // WindowsMaxPath is 32K and although this is called only once we can have a lot of storage environments
            if (GetPath(256, out var realPath) == false)
            {
                if (GetPath((uint)WindowsMaxPath, out realPath) == false)
                    return null;
            }

            //The string that is returned by this function uses the \?\ syntax
            if (realPath.Length >= 8 && realPath.StartsWith("\\\\?\\UNC\\", StringComparison.OrdinalIgnoreCase))
            {
                // network path, replace `\\?\UNC\` with `\\`
                realPath = "\\" + realPath.Substring(7);
            }

            if (realPath.Length >= 4 && realPath.StartsWith("\\\\?\\", StringComparison.OrdinalIgnoreCase))
            {
                // local path, remove `\\?\`
                realPath = realPath.Substring(4);
            }

            return realPath;
        }

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
            out ulong lpFreeBytesAvailable,
            out ulong lpTotalNumberOfBytes,
            out ulong lpTotalNumberOfFreeBytes);

        private const uint FILE_READ_EA = 0x0008;
        private const uint FILE_FLAG_BACKUP_SEMANTICS = 0x2000000;
        private static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateFile(
            [MarshalAs(UnmanagedType.LPTStr)] string filename,
            [MarshalAs(UnmanagedType.U4)] uint access,
            [MarshalAs(UnmanagedType.U4)] FileShare share,
            IntPtr securityAttributes, // optional SECURITY_ATTRIBUTES struct or IntPtr.Zero
            [MarshalAs(UnmanagedType.U4)] FileMode creationDisposition,
            [MarshalAs(UnmanagedType.U4)] uint flagsAndAttributes,
            IntPtr templateFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr hObject);

        [DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern unsafe uint GetFinalPathNameByHandle(
            IntPtr hFile,
            char* buffer,
            uint bufferLength,
            uint dwFlags);
    }

    public class DiskSpaceResult : DriveInfoBase
    {
        public string VolumeLabel { get; set; }

        public Size TotalFreeSpace { get; set; }

        public Size TotalSize { get; set; }
    }

    public class DriveInfoBase
    {
        public string DriveName { get; set; }
    }
}
