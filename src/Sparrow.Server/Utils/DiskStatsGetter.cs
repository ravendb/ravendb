using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;

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
                    state = new State {Drive = drive};
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
            var read = (currentInfo.IoReadOperations - state.Prev.Raw.IoReadOperations) / diff;
            var write = (currentInfo.IoWriteOperations - state.Prev.Raw.IoWriteOperations) / diff;
            var diskSpaceResult = new DiskStatsResult { IoReadOperations = read, IoWriteOperations = write };

            return new PrevInfo { Raw = currentInfo, Calculated = diskSpaceResult };
        }

        class State
        {
            public string Drive;
            public PrevInfo Prev;
        }

        private DiskStatsRawResult GetDiskInfo(string path)
        {
            // https://github.com/whotwagner/statx-fun/blob/master/statx.h
            if (Syscall.statx(0, path, 0, 0x00000fffU, out var buf) != 0)
            {
                if(Logger.IsInfoEnabled)
                    Logger.Info($"Could not get statx of {path} - {Marshal.GetLastWin32Error()}");
                return null;
            }

            var statPath = $"/sys/dev/block/{buf.stx_dev_major}:{buf.stx_dev_minor}/stat";
            using var reader = File.OpenRead(statPath);
            
            return ReadParse(reader);
        }

        private static DiskStatsRawResult ReadParse(FileStream buffer)
        {
            const int maxLongLength = 19;
            const int maxValuesLength = 17;

            Span<char> serializedValue = stackalloc char[maxLongLength];
            Span<long> values = stackalloc long[maxValuesLength];

            var time = DateTime.UtcNow;
            
            int valuesIndex = 0;
            while (buffer.Position < buffer.Length && valuesIndex < maxValuesLength)
            {
                var ch = (char)buffer.ReadByte();
                if (char.IsWhiteSpace(ch))
                    continue;

                var index = 0;
                while (buffer.Position < buffer.Length)
                {
                    serializedValue[index++] = ch;
                    ch = (char)buffer.ReadByte();

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
            if (valuesIndex >= 11) {
                /* Device or partition */
                return new DiskStatsRawResult { IoReadOperations = values[0], IoWriteOperations = values[4], Time = time};
            }
            if (valuesIndex == 4) {
                /* Partition without extended statistics */
                return new DiskStatsRawResult { IoReadOperations = values[0], IoWriteOperations = values[2], Time = time};
            }

            if(Logger.IsInfoEnabled)
                Logger.Info($"The stats file {buffer.Name} should contain at least 4 values");
            return null;
        }
        
        private record DiskStatsRawResult
        {
            public long IoReadOperations { get; init; }
    
            public long IoWriteOperations { get; init; }
    
            public DateTime Time { get; init;}
        }

        class PrevInfo
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
    }

    internal class NotImplementedDiskStatsGetter : IDiskStatsGetter
    {
        public DiskStatsResult Get(string drive) => null;
        public Task<DiskStatsResult> GetAsync(string drive) => null;
    }    
}
