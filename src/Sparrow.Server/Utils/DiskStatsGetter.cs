using System;
using System.Collections.Concurrent;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Logging;
using Mono.Unix.Native;

namespace Sparrow.Server.Utils
{
    public interface IDiskStatsGetter
    {
        DiskStatsResult Get(string drive);
        Task<DiskStatsResult> GetAsync(string drive);
    }

    internal class DiskStatsGetter : IDiskStatsGetter
    {
        private readonly TimeSpan _minInterval;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(DiskStatsGetter).FullName);

        private readonly ConcurrentDictionary<string, Task<PrevInfo>> _previousInfo = new ConcurrentDictionary<string, Task<PrevInfo>>();

        public DiskStatsGetter(TimeSpan minInterval)
        {
            _minInterval = minInterval;
        }

        public DiskStatsResult Get(string drive) => GetAsync(drive).ConfigureAwait(false).GetAwaiter().GetResult();
        
        public async Task<DiskStatsResult> GetAsync(string drive)
        {
            if (drive == null)
                return null;

            var start = DateTime.UtcNow;
            State state = null;
            while (true)
            {
                if (_previousInfo.TryGetValue(drive, out var prevTask) == false)
                {
                    state ??= new State {Drive = drive};
                    var task = new Task<PrevInfo>(GetStats, state);
                    if (_previousInfo.TryAdd(drive, task) == false)
                        continue;

                    task.Start();
                    return null;
                }

                var prev = await prevTask.ConfigureAwait(false);
                if (prev == PrevInfo.Empty)
                {
                    _previousInfo.TryRemove(new KeyValuePair<string, Task<PrevInfo>>(drive, prevTask));
                    continue;
                }
                
                var diff = DateTime.UtcNow - prev.Raw.Time;
                if (start < prev.Raw.Time || diff < _minInterval)
                    return prev.Calculated;

                state ??= new State { Drive = drive };
                state.Prev = prev;

                var calculateTask = new Task<PrevInfo>(CalculateStats, state);
                if (_previousInfo.TryUpdate(drive, calculateTask, prevTask) == false)
                    continue;

                calculateTask.Start();
                return (await calculateTask.ConfigureAwait(false)).Calculated;
            }
        }

        private PrevInfo GetStats(object o)
        {
            var state = (State)o;
            var currentInfo = GetDiskInfo(state.Drive);
            if (currentInfo == null)
                return PrevInfo.Empty;
            
            return new PrevInfo { Raw = currentInfo };
        }

        private PrevInfo CalculateStats(object o)
        {
            var state = (State)o;
            var currentInfo = GetDiskInfo(state.Drive);
            if (currentInfo == null)
                return PrevInfo.Empty;

            var diff = (currentInfo.Time - state.Prev.Raw.Time).TotalSeconds;
            var diskSpaceResult = new DiskStatsResult
            {
                IoReadOperations = (currentInfo.IoReadOperations - state.Prev.Raw.IoReadOperations) / diff, 
                IoWriteOperations = (currentInfo.IoWriteOperations - state.Prev.Raw.IoWriteOperations) / diff, 
                ReadThroughput = new Size((long)((currentInfo.ReadSectors - state.Prev.Raw.ReadSectors) * 512 / diff), SizeUnit.Bytes), 
                WriteThroughput = new Size((long)((currentInfo.WriteSectors - state.Prev.Raw.WriteSectors) * 512 / diff), SizeUnit.Bytes), 
                QueueLength = currentInfo.QueueLength
            };

            return new PrevInfo { Raw = currentInfo, Calculated = diskSpaceResult };
        }

        private class State
        {
            public string Drive;
            public PrevInfo Prev;
        }

        private DiskStatsRawResult GetDiskInfo(string path)
        {
            try
            {
                var (major, minor) = GetDiskMajorMinor(path);

                var statPath = $"/sys/dev/block/{major}:{minor}/stat";
                using var reader = File.OpenRead(statPath);
            
                return ReadParse(reader);
            }
            catch (Exception e)
            {
                if(Logger.IsInfoEnabled)
                    Logger.Info($"Could not get GetDiskInfo of {path}", e);
                return null;
            }
        }

        private (ulong Major, ulong Minor) GetDiskMajorMinor(string path)
        {
            if (Syscall.stat(path, out var stats) != 0)
            {
                var errno = Stdlib.GetLastError();
                var errorBuilder = new StringBuilder();
                Syscall.strerror_r(errno, errorBuilder,1024);
                errorBuilder.Insert(0, $"Failed to get stat for \"{path}\" : ");
                throw new InvalidOperationException(errorBuilder.ToString());
            }
            
            var deviceId = (stats.st_mode & FilePermissions.S_IFBLK) == FilePermissions.S_IFBLK
                ? stats.st_rdev
                : stats.st_dev;
                
            //https://sites.uclouvain.be/SystInfo/usr/include/sys/sysmacros.h.html
            var major = (deviceId & 0x00000000000fff00u) >> 8;
            major |= (deviceId & 0xfffff00000000000u) >> 32;
                
            var minor = (deviceId & 0x00000000000000ffu);
            minor |= (deviceId & 0x00000ffffff00000u) >> 12;
            return (major, minor);
        }

        private static DiskStatsRawResult ReadParse(FileStream fileStream)
        {
            const int maxLongLength = 19;
            const int maxValuesLength = 17;

            Span<char> serializedValue = stackalloc char[maxLongLength];
            Span<long> values = stackalloc long[maxValuesLength];

            var time = DateTime.UtcNow;
            
            int valuesIndex = 0;
            while (fileStream.Position < fileStream.Length && valuesIndex < maxValuesLength)
            {
                int readByte = fileStream.ReadByte();
                if(readByte == -1)
                    //end of file
                    break;
                
                var ch = (char)readByte;
                if (char.IsWhiteSpace(ch))
                    continue;

                var index = 0;
                while (fileStream.Position < fileStream.Length)
                {
                    serializedValue[index++] = ch;
                    readByte = fileStream.ReadByte();
                    if(readByte == -1)
                        //end of file
                        break;
                    
                    ch = (char)readByte;
                    if (char.IsWhiteSpace(ch))
                        break;
                }

                if (long.TryParse(serializedValue[..index], out var value) == false)
                    throw new InvalidOperationException($"Failed to parse {new string(serializedValue[..index])} to number");

                values[valuesIndex++] = value;
            }

            /*
             *https://www.kernel.org/doc/Documentation/block/stat.txt
             *https://github.com/sysstat/sysstat/blob/master/iostat.c#L429
             */
            int ioWriteOperationsIndex;
            int readSectorsIndex;
            int writeSectorsIndex;
            long? queueLength = null;
            if (valuesIndex >= 11) {
                /* Device or partition */
                ioWriteOperationsIndex = 4;
                readSectorsIndex = 2;
                writeSectorsIndex = 6;
                queueLength = values[8];
            }
            else if (valuesIndex == 4) {
                /* Partition without extended statistics */
                ioWriteOperationsIndex = 2;
                readSectorsIndex = 1;
                writeSectorsIndex = 3;
            }
            else
            {
                if(Logger.IsInfoEnabled)
                    Logger.Info($"The stats file {fileStream.Name} should contain at least 4 values");
                return null;
            }

            return new DiskStatsRawResult
            {
                IoReadOperations = values[0], 
                IoWriteOperations = values[ioWriteOperationsIndex],
                ReadSectors = values[readSectorsIndex],
                WriteSectors = values[writeSectorsIndex],
                QueueLength = queueLength,
                
                Time = time
            };
        }
        
        private record DiskStatsRawResult
        {
            public long IoReadOperations { get; init; }
            public long IoWriteOperations { get; init; }
            public long ReadSectors { get; set; }
            public long WriteSectors { get; set; }
            public long? QueueLength { get; set; }
            public DateTime Time { get; init;}
        }

        private class PrevInfo
        {
            public static PrevInfo Empty = new PrevInfo();
            public DiskStatsResult Calculated { get; set; }
            public DiskStatsRawResult Raw { get; set; }
        }
    }
    
    public record DiskStatsResult
    {
        public double IoReadOperations { get; init; }
        public double IoWriteOperations { get; init;}
        public Size ReadThroughput { get; set; }
        public Size WriteThroughput { get; set; }
        public long? QueueLength { get; set; }
    }

    internal class NotImplementedDiskStatsGetter : IDiskStatsGetter
    {
        public DiskStatsResult Get(string drive) => null;
        public Task<DiskStatsResult> GetAsync(string drive) => null;
    }    
}
