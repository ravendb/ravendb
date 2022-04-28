using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Server.Platform;

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
                var diff = DateTime.UtcNow - prev.Raw.Time;
                if (start < prev.Raw.Time || diff < _minInterval)
                    return prev.Calculated;

                state ??= new State {Drive = drive};
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
            return currentInfo != null ? new PrevInfo {Raw = currentInfo} : null;
        }

        private PrevInfo CalculateStats(object o)
        {
            var state = (State)o;
            var currentInfo = GetDiskInfo(state.Drive);
            if (currentInfo == null)
                return null;

            var diff = (currentInfo.Time - state.Prev.Raw.Time).TotalSeconds;
            var read = (currentInfo.ReadIOs - state.Prev.Raw.ReadIOs) / diff;
            var write = (currentInfo.WriteIOs - state.Prev.Raw.WriteIOs) / diff;
            var diskSpaceResult = new DiskStatsResult {ReadIos = read, WriteIos = write};

            return new PrevInfo {Raw = currentInfo, Calculated = diskSpaceResult};
        }

        class State
        {
            public string Drive;
            public PrevInfo Prev;
        }

        private DiskStatsRawResult GetDiskInfo(string path)
        {
            var result = Pal.rvn_get_path_disk_stats(path, out var ioStats, out var error);
            if (result == PalFlags.FailCodes.Success)
                return new DiskStatsRawResult {Time = DateTime.UtcNow, ReadIOs = (long)ioStats.Read, WriteIOs = (long)ioStats.Write};
            
            if (Logger.IsInfoEnabled)
                Logger.Info(PalHelper.GetNativeErrorString(error, $"Failed to get file system statistics for path: {path}. FailCode={result}", out _));
         
            return null;
        }
        
        private record DiskStatsRawResult
        {
            public long ReadIOs { get; init; }
    
            public long WriteIOs { get; init; }
    
            public DateTime Time { get; init;}
        }

        class PrevInfo
        {
            public DiskStatsResult Calculated { get; set; }
            public DiskStatsRawResult Raw { get; set; }
        }
    }
    
    public record DiskStatsResult
    {
        public double ReadIos { get; init; }
    
        public double WriteIos { get; init;}
    }

    internal class NotImplementedDiskStatsGetter : IDiskStatsGetter
    {
        public DiskStatsResult Get(string drive) => null;
        public Task<DiskStatsResult> GetAsync(string drive) => null;
    }    
}
