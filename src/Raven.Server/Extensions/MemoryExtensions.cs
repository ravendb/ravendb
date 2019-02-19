using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Extensions
{
    public static class MemoryExtensions
    {
        public static void SetWorkingSet(Process process, double ramInGb, Logger logger)
        {
#if SET_WORKING_SET
            var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
            if (memoryInfo.UsableMemory < ramInGb)
            {
                ramInGb = memoryInfo.UsableMemory;
            }

            var maxWorkingSetInBytes = (long)Size.ConvertToBytes(ramInGb, SizeUnit.Gigabytes);
            var minWorkingSetInBytes = process.MinWorkingSet.ToInt64();
            if (minWorkingSetInBytes > maxWorkingSetInBytes)
            {
                minWorkingSetInBytes = maxWorkingSetInBytes;
            }

            if (PlatformDetails.RunningOnPosix == false)
            {
                // windows
                const QuotaLimit flags = QuotaLimit.QUOTA_LIMITS_HARDWS_MAX_DISABLE;
                var result = SetProcessWorkingSetSizeEx(process.Handle, minWorkingSetInBytes, minWorkingSetInBytes, flags);
                if (result == false)
                {
                    logger.Info($"Failed to set max working set to {ramInGb}, error code: {Marshal.GetLastWin32Error()}");
                }

                return;
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                // macOS
                process.MinWorkingSet = new IntPtr(minWorkingSetInBytes);
                process.MaxWorkingSet = new IntPtr(maxWorkingSetInBytes);
                return;
            }

            const string groupName = "ravendb";
            var basePath = $"/sys/fs/cgroup/memory/{groupName}";
            var fd = Syscall.open(basePath, 0, 0);
            if (fd == -1)
            {
                if (Syscall.mkdir(basePath, (ushort)FilePermissions.S_IRWXU) == -1)
                {
                    logger.Info($"Failed to create directory path: {basePath}, error code: {Marshal.GetLastWin32Error()}");
                    return;
                }
            }

            Syscall.close(fd);

            var str = maxWorkingSetInBytes.ToString();
            if (WriteValue($"{basePath}/memory.limit_in_bytes", str, logger) == false)
                return;

            WriteValue($"{basePath}/cgroup.procs", str, logger);
#endif
        }

        private static unsafe bool WriteValue(string path, string str, Logger logger)
        {
            var fd = Syscall.open(path, OpenFlags.O_WRONLY, FilePermissions.S_IWUSR);
            if (fd == -1)
            {
                logger.Info($"Failed to open path: {path}");
                return false;
            }

            fixed (char* x = str)
            {
                var length = str.Length;
                while (length > 0)
                {
                    var written = Syscall.write(fd, x, (ulong)length);
                    if (written <= 0)
                    {
                        // -1 or 0 is error when not regular file, 
                        // and this is a case of non-regular file
                        logger.Info($"Failed to write to path: {path}, value: {str}");
                        Syscall.close(fd);
                        return false;
                    }
                    length -= (int)written;
                }

                if (Syscall.close(fd) == -1)
                {
                    logger.Info($"Failed to close: {path}");
                    return false;
                }
            }

            return true;
        }

        public static void EmptyWorkingSet(Logger logger)
        {
            if (PlatformDetails.RunningOnPosix)
                return;

            using (var process = Process.GetCurrentProcess())
            {
                const QuotaLimit flags = QuotaLimit.QUOTA_LIMITS_HARDWS_MAX_ENABLE;
                var result = SetProcessWorkingSetSizeEx(process.Handle, -1, -1, flags);
                if (result == false)
                {
                    logger?.Info($"Failed to empty working set, error code: {Marshal.GetLastWin32Error()}");
                }
            }
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", EntryPoint = "SetProcessWorkingSetSize", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        internal static extern bool SetProcessWorkingSetSizeEx(IntPtr pProcess,
            long dwMinimumWorkingSetSize, long dwMaximumWorkingSetSize, QuotaLimit flags);

        [Flags]
        internal enum QuotaLimit
        {
            QUOTA_LIMITS_HARDWS_MIN_DISABLE = 0x00000002,
            QUOTA_LIMITS_HARDWS_MIN_ENABLE = 0x00000001,
            QUOTA_LIMITS_HARDWS_MAX_DISABLE = 0x00000008,
            QUOTA_LIMITS_HARDWS_MAX_ENABLE = 0x00000004
        }
    }
}
