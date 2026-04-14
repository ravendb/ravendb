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
        /// Marks a file as sparse. On Windows calls FSCTL_SET_SPARSE; on POSIX unwritten regions are
        /// automatically holes so no setup is needed. Returns false if sparse is not supported.
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
