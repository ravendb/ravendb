// -----------------------------------------------------------------------
//  <copyright file="NativeMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Util.Settings;

namespace Voron.Platform.Posix
{
    public class PosixHelper
    {
        public static unsafe void AllocateFileSpace(StorageEnvironmentOptions options, int fd, long size, string file)
        {
            bool usingLseek;
            var result = Syscall.AllocateFileSpace(fd, size, file, out usingLseek);
            
            if (result == (int)Errno.ENOSPC)
            {
                var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(file);

                // Use Pal's detailed error string (until PosixHelper will be entirely removed)
                var nativeMsg = PalHelper.GetNativeErrorString(result, "Failed to AllocateFileSpace (PosixHelper)", out _);

                throw new DiskFullException(file, size, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), nativeMsg);
            }
            if (result != 0)
                Syscall.ThrowLastError(result, $"posix_fallocate(\"{file}\", {size})");
        }

        public static string GetFileSystemOfPath(string path)
        {
            var allMounts = DriveInfo.GetDrives();
            string filesystem = "Unresolved";
            var matchSize = 0;
            foreach (var m in allMounts)
            {
                var mountNameSize = m.Name.Length;
                if (path.StartsWith(m.Name))
                {
                    if (mountNameSize > matchSize)
                    {
                        matchSize = mountNameSize;
                        filesystem = m.DriveType == DriveType.Unknown ? "Unknown" : m.DriveFormat;
                        // do not break foreach statement to get longest substring path match
                    }
                }
            }
            return filesystem;
        }

   

        public static unsafe bool TryReadFileHeader(FileHeader* header, VoronPathSetting path)
        {
            var fd = Syscall.open(path.FullPath, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
            try
            {
                if (fd == -1)
                {
                    var lastError = Marshal.GetLastWin32Error();
                    if (((Errno) lastError) == Errno.EACCES)
                        return false;
                    Syscall.ThrowLastError(lastError);
                }
                int remaining = sizeof(FileHeader);
                var ptr = ((byte*) header);
                while (remaining > 0)
                {
                    var read = Syscall.read(fd, ptr, (ulong) remaining);
                    if (read == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        Syscall.ThrowLastError(err);
                    }

                    if (read == 0)
                        return false; // truncated file?

                    remaining -= (int) read;
                    ptr += read;
                }
                return true;
            }
            finally
            {
                if (fd != -1)
                {
                    Syscall.close(fd);
                    fd = -1;
                }
            }
        }

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

        public static void EnsurePathExists(string file)
        {
            var dirpath = Path.GetDirectoryName(file);
            List<string> dirsToCreate = new List<string>();
            while (Directory.Exists(dirpath) == false)
            {
                dirsToCreate.Add(dirpath);
                dirpath = Directory.GetParent(dirpath).ToString();
                if (dirpath == null)
                    break;
            }
            dirsToCreate.ForEach(x => Directory.CreateDirectory(x));
        }
    }
}
