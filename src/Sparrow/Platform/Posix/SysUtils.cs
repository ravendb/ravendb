using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Voron.Platform.Posix;

namespace Sparrow.Platform.Posix
{
    public static class SysUtils
    {
        private static Logger _logger = LoggingSource.Instance.GetLogger("SysUtils", "Raven/Server");

        public static unsafe ulong ReadULongFromFile(string filename)
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
            try
            {
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
            }
            finally
            {
                Marshal.FreeHGlobal(pBuf);
            }
            return cgroup;
        }

        public static unsafe List<int> ReadAndParseRangesFromFile(string filename)
        {
            var fd = Syscall.open(filename, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
            if (fd < 0)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Cannot open '{filename}'. Will not respect container's number of cores if running under one.");
                return null;
            }

            List<int> coresList;
            UIntPtr readSize = (UIntPtr)1024;
            IntPtr pBuf = Marshal.AllocHGlobal((int)readSize);
            try
            {
                Memory.Set((byte*)pBuf, 0, 1024);
                var cgroupRead = Syscall.read(fd, pBuf.ToPointer(), (ulong)readSize);
                if (cgroupRead > 1000 || cgroupRead == 0) // check we are not garbadged
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info(
                            $"Got invalid number of characters ({cgroupRead}) from filename. Will not respect container's number of cores if running under one.");
                    Syscall.close(fd);
                    return null;
                }

                Syscall.close(fd);

                string str = null;
                try
                {
                    str = Encoding.ASCII.GetString((byte*)pBuf.ToPointer(), (int)cgroupRead);
                    coresList = ParseRangesFromString(str);
                }
                catch (Exception ex)
                {

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"couldn't convert string '{str}' to long. Will not respect container's number of cores if running under one.", ex);
                    return null;
                }
            }
            finally
            {
                Marshal.FreeHGlobal(pBuf);
            }
            return coresList;
        }

        private static List<int> ParseRangesFromString(string str)
        {
            var result = new List<int>();
            foreach (var corerange in str.Split(','))
            {
                var lowhigh = corerange.Split('-');
                if (lowhigh.Length > 2)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"strange range in '{str}' => '{lowhigh.Length}. Will not respect container's number of cores if running under one.");
                    return null;
                }
                if (lowhigh.Length == 2)
                {
                    int[] lowhighInt = new int[2];
                    try
                    {
                        lowhighInt[0] = Convert.ToInt32(lowhigh[0]);
                        lowhighInt[1] = Convert.ToInt32(lowhigh[1]);
                    }
                    catch (Exception ex)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"cannot convert range to int in '{str}' => '{lowhigh[0]} - {lowhigh[1]}'. Will not respect container's number of cores if running under one.", ex);
                        return null;
                    }
                    for (var i = lowhighInt[0]; i <= lowhighInt[1]; i++)
                    {
                        result.Add(i);
                    }
                }
                else
                {
                    try
                    {
                        var core = Convert.ToInt32(lowhigh[0]);
                        result.Add(core);
                    }
                    catch (Exception ex)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"cannot convert core number to int in '{str}' => '{lowhigh[0]}'. Will not respect container's number of cores if running under one.", ex);
                        return null;
                    }
                }
            }
            return result;
        }
    }
}
