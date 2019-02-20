using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Sparrow.Platform.Posix
{
    internal static class KernelVirtualFileSystemUtils
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(KernelVirtualFileSystemUtils).FullName);
        private static readonly ConcurrentSet<string> IsOldFileAlert = new ConcurrentSet<string>();

        public static long? ReadNumberFromCgroupFile(string fileName)
        {
            try
            {
                var txt = File.ReadAllText(fileName);
                var result = Convert.ToInt64(txt);
                if (result <= 0)
                    return null;

                return result;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.TryAdd(fileName) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{fileName}', will not respect container's limit", e);
                }
                return null;
            }
        }

        public static int ReadNumberOfCoresFromCgroupFile(string filename)
        {
            // on error or if not applicable - return Environment.ProcessorCount
            try
            {
                var txt = File.ReadAllText(filename);
                // cpuset.cpus output format is a ranges of used core numbers comma seperated list. 
                // i.e. : 3,5-7,9 (which are 3,5,6,7,9 and the result should be 5 cores)
                // it is guaranteed to get the cores numbers and ranges from lower to upper
                var coresCount = 0;
                foreach (var coreRange in txt.Split(','))
                {
                    var cores = coreRange.Split('-');
                    if (cores.Length == 1)
                        coresCount++;
                    else
                        coresCount += Convert.ToInt32(cores[1]) - Convert.ToInt32(cores[0]) + 1;  // "5-7" == 3
                }
                return coresCount;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{filename}', will not respect container's number of cores", e);
                }
                return Environment.ProcessorCount;
            }
        }

        public static (string DeviceName, bool IsDeviceSwapFile)[] ReadSwapInformationFromSwapsFile()
        {
            // /proc/swaps output format :
            //              Filename        Type            Size            Used    Priority
            //              /dev/sda5       partition       16691196        109376  -1
            //                   .
            //                   .


            // on error or if not applicable - return null path array
            var filename = "/proc/swaps";
            try
            {
                var txt = File.ReadAllText(filename);
                var items = System.Text.RegularExpressions.Regex.Split(txt, @"\s+").Where(s => s != string.Empty).ToArray();

                if (items.Length < 6)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"no swap defined on this system according to {filename}");
                    return Array.Empty<(string,bool)>();
                }

                if (items[0].Equals("Filename") == false ||
                    items[1].Equals("Type") == false ||
                    items[2].Equals("Size") == false ||
                    items[3].Equals("Used") == false ||
                    items[4].Equals("Priority") == false)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Unrecognized header at {filename}, cannot read swap information");
                    return Array.Empty<(string, bool)>();
                }


                if (items.Length % 5 != 0)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Invalid number of fields at {filename}, cannot read swap information");
                    return Array.Empty<(string, bool)>();
                }

                var numberOfSwaps = items.Length / 5 - 1; // "-1" ignore header;
                if (numberOfSwaps < 1)
                    return Array.Empty<(string, bool)>(); // no swaps defined

                var swapDevices = new (string, bool)[numberOfSwaps];                

                int j = 0;
                for (var i = 5; i < items.Length; i += 5) // start from "5" - skip header
                {
                    swapDevices[j] = (items[i], items[i + 1].Contains("file"));
                }

                return swapDevices;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                    Logger.Operations($"Unable to read and parse '{filename}', cannot read swap information", e);
                return Array.Empty<(string, bool)>();
            }
        }

        public static long ReadNumberFromFile(string filename)
        {
            // return long number read from file, on error return long.MaxValue
            try
            {
                var txt = File.ReadAllText(filename);
                var result = Convert.ToInt64(txt);
                return result;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{filename}'", e);
                }
                return long.MaxValue;
            }
        }

        public static HashSet<string> GetAllDisksFromPartitionsFile()
        {
            // /proc/partitions output format :
            //          major minor  #blocks  name

            //          8        0  125033783 sda
            //          8        1  108340224 sda1
            //          8        2          1 sda2
            //          8        5   16691200 sda5
            //          8       16  488386584 sdb

            // on error or if not applicable - return empty hash set
            var filename = "/proc/partitions";
            var results = new HashSet<string>();
            try
            {
                var txt = File.ReadAllText(filename);
                var items = System.Text.RegularExpressions.Regex.Split(txt, @"\s+").Where(s => s != string.Empty).ToArray();

                if (items.Length < 5)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"no partitions defined on this system according to {filename}");
                    return results;
                }

                if (items[0].Equals("major") == false ||
                    items[1].Equals("minor") == false ||
                    items[2].Equals("#blocks") == false ||
                    items[3].Equals("name") == false)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Unrecognized header at {filename}, cannot read partitions information");
                    return results;
                }

                if (items.Length % 4 != 0)
                {
                    if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Invalid number of fields at {filename}, cannot read partitions information");
                    return results;
                }

                for (var i = 7; i < items.Length; i += 4) // start from "7" - skip header
                {
                    // remove numbers at end of string (i.e.: /dev/sda5 ==> sda)
                    var reg = new System.Text.RegularExpressions.Regex(@"\d+$"); // we do not check swap file, only partitions
                    var disk = reg.Replace(items[i], "").Replace("/dev/", "");
                    if (disk != string.Empty)
                        results.Add(disk); // hash set to avoid duplicates (i.e. sda1 && sda)
                }

                return results;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.TryAdd(filename) && Logger.IsOperationsEnabled)
                    Logger.Operations($"Unable to read and parse '{filename}', cannot read partitions information", e);
                return new HashSet<string>();
            }
        }

        public static string ReadLineFromFile(string path, string filter)
        {
            try
            {
                string result = null;
                var txt = File.ReadAllLines(path);
                var cnt = 0;
                foreach (var line in txt)
                {
                    if (line.Contains(filter))
                    {
                        result = line;
                        if (++cnt > 1)
                            return null;
                    }
                }
                return result;
            }
            catch (Exception)
            {
                return null;
            }
        }


        public class BufferedPosixKeyValueOutputValueReader : IDisposable
        {
            private readonly string _path;
            private byte[] _buffer;
            private int _bytesRead;

            public BufferedPosixKeyValueOutputValueReader(string path)
            {
                _path = path;
                _buffer = ArrayPool<byte>.Shared.Rent(8 * 1024);
            }
            
            public void ReadFileIntoBuffer()
            {

                using (var fileStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    if (fileStream.Length > _buffer.Length)
                        ThrowStreamBiggerThenBufferException();

                    _bytesRead = 0;
                    

                    while (true)
                    {
                        var read = fileStream.Read(_buffer, _bytesRead, _buffer.Length - _bytesRead);
                        if (read == 0)
                            break;
                        _bytesRead += read;
                    }    
                }
            }

            private (int Start, int End) SearchInBuffer(byte[] filter)
            {
                var maxValidSize = _bytesRead - filter.Length;
                for (int j = 0; j < maxValidSize; j++)
                {
                    if (filter[0] == _buffer[j])
                    {
                        bool success = true;
                        for (int k = 1; k < filter.Length; k++)
                        {
                            if (filter[k] != _buffer[j + k])
                            {
                                success = false;
                                break;
                            }
                        }

                        if (success == false)
                        {
                            while (++j < _bytesRead)
                            {
                                if (_buffer[j] == '\n')
                                    break;
                            }
                            continue;
                        }

                        var start = j + filter.Length;
                        while (
                            start < _bytesRead && 
                            (_buffer[start] == ' ' || _buffer[start] == '\t'))
                        {
                            start++;
                        }

                        var end = start;
                        while (end < _bytesRead && _buffer[end] != '\n')
                        {
                            end++;
                        }

                        return (start, end);
                    }
                }

                return (0, 0);
            }
            
            public long ExtractNumericValueFromKeyValuePairsFormattedFile(byte[] filter)
            {
                var (start, end) = SearchInBuffer(filter);
                long value = 0;
                for (int i = start; i < end; i++)
                {
                    if (_buffer[i] < (byte)'0' || _buffer[i] > (byte)'9')
                        return value;
                    
                    value *= 10;
                    value += _buffer[i] - (byte)'0';
                }
                return value;
            }

            private void ThrowStreamBiggerThenBufferException()
            {
                throw new InvalidOperationException($"The stream for {_path} is bigger then the buffer");
            }

            public void Dispose()
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = null;
            }
        }
        
    }
}
