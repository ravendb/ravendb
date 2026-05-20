using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Mono.Unix.Native;
using Sparrow.Platform;

namespace Voron.Platform;

internal static class HardLinkInfo
{
    public static long GetLinkCount(string path)
    {
        if (PlatformDetails.RunningOnPosix)
            return GetLinkCountPosix(path);
        return GetLinkCountWindows(path);
    }

    private static long GetLinkCountPosix(string path)
    {
        if (Syscall.stat(path, out Stat st) != 0)
        {
            Errno errno = Stdlib.GetLastError();
            throw new IOException($"stat() failed for '{path}': errno={(int)errno} ({errno})");
        }
        return (long)st.st_nlink;
    }

    private static long GetLinkCountWindows(string path)
    {
        using SafeFileHandle handle = File.OpenHandle(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.None);
        if (GetFileInformationByHandle(handle, out BY_HANDLE_FILE_INFORMATION info) == false)
            throw new Win32Exception(Marshal.GetLastWin32Error(), $"GetFileInformationByHandle failed for '{path}'");
        return info.nNumberOfLinks;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

    [StructLayout(LayoutKind.Sequential)]
    private struct BY_HANDLE_FILE_INFORMATION
    {
        public uint dwFileAttributes;
        public FILETIME ftCreationTime;
        public FILETIME ftLastAccessTime;
        public FILETIME ftLastWriteTime;
        public uint dwVolumeSerialNumber;
        public uint nFileSizeHigh;
        public uint nFileSizeLow;
        public uint nNumberOfLinks;
        public uint nFileIndexHigh;
        public uint nFileIndexLow;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct FILETIME
    {
        public uint dwLowDateTime;
        public uint dwHighDateTime;
    }
}
