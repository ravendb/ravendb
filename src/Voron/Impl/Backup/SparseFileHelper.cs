using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Platform;

namespace Voron.Impl.Backup
{
    internal static class SparseFileHelper
    {
        private const uint FSCTL_SET_SPARSE = 0x000900c4;

        /// <summary>
        /// Prepares a file for sparse-aware restore logic.
        /// On Windows, this calls FSCTL_SET_SPARSE. On POSIX (Linux/macOS), unwritten
        /// regions are left unwritten and the filesystem may account for them as holes,
        /// so there is no additional setup required here.
        /// Returns false if sparse files are not supported on this filesystem.
        /// </summary>
        public static bool TryMarkFileAsSparse(FileStream file)
        {
            if (PlatformDetails.RunningOnPosix)
                return true;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return false;

            return MarkSparseWindows(file);
        }

        private static bool MarkSparseWindows(FileStream file)
        {
            try
            {
                bool result = DeviceIoControl(
                    file.SafeFileHandle.DangerousGetHandle(),
                    FSCTL_SET_SPARSE,
                    IntPtr.Zero,
                    0,
                    IntPtr.Zero,
                    0,
                    out _,
                    IntPtr.Zero);

                return result;
            }
            catch
            {
                // If DeviceIoControl fails (e.g., unsupported filesystem like FAT32),
                // fall back to normal (non-sparse) file writing
                return false;
            }
        }

        [DllImport("Kernel32.dll", SetLastError = true)]
        private static extern bool DeviceIoControl(
            IntPtr hDevice,
            uint IoControlCode,
            IntPtr InBuffer,
            uint InBufferSize,
            IntPtr OutBuffer,
            int OutBufferSize,
            out int BytesReturned,
            IntPtr Overlapped);
    }
}
