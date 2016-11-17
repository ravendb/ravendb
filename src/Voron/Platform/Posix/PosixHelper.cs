// -----------------------------------------------------------------------
//  <copyright file="NativeMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
    public class PosixHelper
    {
        public static void AllocateFileSpace(int fd, ulong size, string file)
        {
            int result;
            int retries = 1024;
            while (true)
            {
                result = Syscall.posix_fallocate(fd, 0, size);
                if (result != (int)Errno.EINTR)
                    break;
                if (retries-- > 0)
                    throw new IOException("Tried too many times to call posix_fallocate, but always got EINTR, cannot retry again");
            }
            if (result != 0)
                ThrowLastError(result, $"posix_fallocate(\"{file}\", {size})");
        }

        public static void ThrowLastError(int lastError, string msg = null)
        {
            if (Enum.IsDefined(typeof(Errno), lastError) == false)
                throw new InvalidOperationException("Unknown errror " + lastError);
            var error = (Errno)lastError;
            throw new InvalidOperationException(error + " " + msg);
        }

        public static unsafe void WriteFileHeader(FileHeader* header, string path)
        {
            var fd = Syscall.open(path, OpenFlags.O_WRONLY | OpenFlags.O_CREAT,
                                  FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);

            try
            {
                if (fd == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    ThrowLastError(err, "when opening " + path);
                }
                    
                int remaining = sizeof(FileHeader);
                var ptr = ((byte*)header);
                while (remaining > 0)
                {
                    var written = Syscall.write(fd, ptr, (ulong)remaining);
                    if (written == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        ThrowLastError(err, "writing to " + path);
                    }

                    remaining -= (int) written;
                    ptr += written;
                }
                if(Syscall.fsync(fd) == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    ThrowLastError(err, "fsync " + path);
                }
                if (SyncDirectory(path) == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    ThrowLastError(err, "fsync dir " + path);
                }
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

        public static bool CheckSyncDirectoryAllowed(string path)
        { 
            DriveInfo[] allDrives = DriveInfo.GetDrives();

            foreach (DriveInfo d in allDrives)
            {
                if (path.Contains(d.Name))
                {
                    // TODO : Add other types
                    if (d.DriveFormat.Equals("cifs"))
                        return false;
                    return true;
                }
            }
            return true;
        }

        public static int SyncDirectory(string path)
        {
            var dir = Path.GetDirectoryName(path);
            var fd = Syscall.open(dir, 0, 0);
            if (fd == -1)
                return -1;
            var fsyncRc = Syscall.fsync(fd);
            if (fsyncRc == -1)
                return -1;
            return Syscall.close(fd);
        }

        public static unsafe bool TryReadFileHeader(FileHeader* header, string path)
        {
            var fd = Syscall.open(path, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
            try
            {
                if (fd == -1)
                {
                    var lastError = Marshal.GetLastWin32Error();
                    if (((Errno) lastError) == Errno.EACCES)
                        return false;
                    ThrowLastError(lastError);
                }
                int remaining = sizeof(FileHeader);
                var ptr = ((byte*)header);
                while (remaining > 0)
                {
                    var read = Syscall.read(fd, ptr, (ulong)remaining);
                    if (read == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        ThrowLastError(err);
                    }

                    if (read == 0)
                        return false;// truncated file?

                    remaining -= (int)read;
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
