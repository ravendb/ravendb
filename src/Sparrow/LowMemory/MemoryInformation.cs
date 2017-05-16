using System;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow.LowMemory
{
    public static class MemoryInformation
    {
        private static Logger _logger = LoggingSource.Instance.GetLogger<MemoryInfoResult>("Raven/Server");

        private static int _memoryLimit;
        private static bool _failedToGetAvailablePhysicalMemory;
        private static readonly MemoryInfoResult FailedResult = new MemoryInfoResult
        {
            AvailableMemory = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
        };

        [StructLayout(LayoutKind.Sequential)]
        public struct MemoryStatusEx
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll",SetLastError = true)]
        public static extern unsafe bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        /// <summary>
        /// This value is in MB
        /// </summary>
        public static int MemoryLimit
        {
            get { return _memoryLimit; }
            set
            {
                _memoryLimit = value;
            }
        }

        public static unsafe MemoryInfoResult GetMemoryInfo()
        {
            if (_failedToGetAvailablePhysicalMemory)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
                return FailedResult;
            }

            try
            {
                if (PlatformDetails.RunningOnPosix)
                {
                    // get container usage (cgroup) and machine usage (sysinfo) and respect the lower
                    var cgroupLimit = ReadULongFromFile("/sys/fs/cgroup/memory/memory.limit_in_bytes");
                    ulong cgroupAvailable = ulong.MaxValue;
                    if (cgroupLimit != ulong.MaxValue && cgroupLimit < long.MaxValue-(4*1024)) // max limit reported on non container system is usually long.MaxValue - 4K
                    {
                        var cgroupUsage = ReadULongFromFile("/sys/fs/cgroup/memory/memory.usage_in_bytes");
                        cgroupAvailable = cgroupLimit - cgroupUsage;
                    }

                    sysinfo_t info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }


                    if (cgroupLimit < info.TotalRam) // ulong comparison
                    {
                        return new MemoryInfoResult
                        {
                            AvailableMemory = new Size((long)cgroupAvailable, SizeUnit.Bytes),
                            TotalPhysicalMemory = new Size((long)cgroupLimit, SizeUnit.Bytes),
                            RunningInContainer = true
                        };
                    }

                    return new MemoryInfoResult
                    {
                        AvailableMemory = new Size((long)info.AvailableRam, SizeUnit.Bytes),
                        TotalPhysicalMemory = new Size((long)info.TotalRam, SizeUnit.Bytes)
                    };
                }

              

                var memoryStatus = new MemoryStatusEx
                {
                    dwLength = (uint)sizeof(MemoryStatusEx)
                };
                var result = GlobalMemoryStatusEx(&memoryStatus);
                if (result == false)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                    return FailedResult;
                }

                return new MemoryInfoResult
                {
                    AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                    TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                };
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                _failedToGetAvailablePhysicalMemory = true;
                return FailedResult;
            }
        }

        private static unsafe ulong ReadULongFromFile(string filename)
        {
            var fd = Syscall.open(filename, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
            if (fd < 0)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Cannot open '{filename}'. Will not respect container's memory limit if running under one.");
                return ulong.MaxValue;
            }

            ulong cgroup;
            UIntPtr readSize = (UIntPtr)32;
            IntPtr pBuf = Marshal.AllocHGlobal((int)readSize);
            Memory.Set((byte*)pBuf, 0, 32);
            var cgroupRead = Syscall.read(fd, pBuf.ToPointer(), (ulong)readSize);
            if (cgroupRead > 30 || cgroupRead == 0) // check we are not garbadged
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Got invalid number of characters ({cgroupRead}) from filename. Will not respect container's memory limit if running under one.");
                Syscall.close(fd);
                return ulong.MaxValue;
            }

            Syscall.close(fd);

            string str = null;
            try
            {
                str = Encoding.ASCII.GetString((byte*)pBuf.ToPointer(), (int)cgroupRead);
                cgroup = Convert.ToUInt64(str);
            }
            catch (Exception ex)
            {

                if (_logger.IsInfoEnabled)
                    _logger.Info($"couldn't convert string '{str}' to long. Will not respect container's memory limit if running under one.", ex);
                cgroup = ulong.MaxValue;
            }
            Marshal.FreeHGlobal(pBuf);
            return cgroup;
        }
    }

    public struct MemoryInfoResult
    {
        public Size TotalPhysicalMemory;
        public Size AvailableMemory;
        public bool RunningInContainer;
    }
}