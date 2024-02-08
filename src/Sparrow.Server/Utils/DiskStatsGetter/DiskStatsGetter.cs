using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils.DiskStatsGetter
{
    internal abstract class DiskStatsGetter<T> : IDiskStatsGetter 
        where T : IDiskStatsRawResult
    {
        private readonly TimeSpan _minInterval;

        private readonly ConcurrentDictionary<string, Task<PrevInfo>> _previousInfo = new ConcurrentDictionary<string, Task<PrevInfo>>();

        protected DiskStatsGetter(TimeSpan minInterval)
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

            var diskSpaceResult = CalculateStats(currentInfo, state);
            return new PrevInfo { Raw = currentInfo, Calculated = diskSpaceResult };
        }

        protected abstract DiskStatsResult CalculateStats(T currentInfo, State state);

        protected class State
        {
            public string Drive;
            public PrevInfo Prev;
        }

        protected abstract T GetDiskInfo(string path);
        
        protected class PrevInfo
        {
            public static PrevInfo Empty = new PrevInfo();
            public DiskStatsResult Calculated { get; set; }
            public T Raw { get; set; }
        }
    }
}
