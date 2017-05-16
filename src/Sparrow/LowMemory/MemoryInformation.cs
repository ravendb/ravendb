using System;
using System.Runtime.InteropServices;
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
                    ulong cgroupLimit = ulong.MaxValue;
                    ulong cgroupUsage = ulong.MaxValue;

                    var fdLimit = Syscall.open("/sys/fs/cgroup/memory/memory.limit_in_bytes", OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
                    var fdUsage = Syscall.open("/sys/fs/cgroup/memory/memory.usage_in_bytes", OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);

                    if (fdLimit > 0)
                    {
                        UIntPtr readSize = (UIntPtr)sizeof(ulong);
                        IntPtr pBuf = Marshal.AllocHGlobal((int)readSize);
                        var cgroupLimitRead = Syscall.read(fdLimit, pBuf.ToPointer(), (ulong)readSize);
                        if (cgroupLimitRead != 0)
                        {
                            Console.WriteLine($"ADIADI :: Strange : cgroupLimitRead = {sizeof(ulong)}");
                        }
                        Syscall.close(fdLimit);
                        cgroupLimit = (ulong)pBuf;
                        Marshal.FreeHGlobal(pBuf);
                    }
                    else
                    {
                        Console.WriteLine($"ADIADI :: Cannot open fdLimit");
                    }


                    if (fdUsage > 0)
                    {
                        UIntPtr readSize = (UIntPtr)sizeof(ulong);
                        IntPtr pBuf = Marshal.AllocHGlobal((int)readSize);
                        var cgroupUsageRead = Syscall.read(fdUsage, pBuf.ToPointer(), (ulong)readSize);
                        if (cgroupUsageRead != 0)
                        {
                            Console.WriteLine($"ADIADI :: Strange : cgroupUsageRead = {sizeof(ulong)}");
                        }
                        Syscall.close(fdUsage);
                        cgroupUsage = (ulong)pBuf;
                        Marshal.FreeHGlobal(pBuf);
                    }
                    else
                    {
                        Console.WriteLine($"ADIADI :: Cannot open fdUsage");
                    }


                    sysinfo_t info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return FailedResult;
                    }


                    Console.WriteLine("Available Memory ( sys  )= " + (long)info.AvailableRam);
                    Console.WriteLine("Available Memory (cgroup)= " + (long)cgroupUsage);
                    Console.WriteLine("Physical  Memory ( sys  )= " + (long)info.TotalRam);
                    Console.WriteLine("Available Memory (cgroup)= " + (long)cgroupLimit);


                    return new MemoryInfoResult
                    {
                        AvailableMemory = new Size((long)info.AvailableRam, SizeUnit.Bytes),
                        TotalPhysicalMemory = new Size((long)info.TotalRam, SizeUnit.Bytes),
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
    }

    public struct MemoryInfoResult
    {
        public Size TotalPhysicalMemory;
        public Size AvailableMemory;
    }
}