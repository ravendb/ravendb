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
        private readonly TimeSpan _maxWait = TimeSpan.FromMilliseconds(100);

        private readonly ConcurrentDictionary<string, DiskStatsCache> _previousInfo = new ConcurrentDictionary<string, DiskStatsCache>();

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
                if (_previousInfo.TryGetValue(drive, out var cache) == false)
                {
                    state ??= new State { Drive = drive };
                    var task = new Task<GetStatsResult>(GetStats, state);
                    if (_previousInfo.TryAdd(drive, new DiskStatsCache { Task = task }))
                        task.Start();
                    return null;
                }

                if (cache.Task.IsCompleted == false)
                {
                    await Task.WhenAny(cache.Task, Task.Delay(_maxWait)).ConfigureAwait(false);
                    if (cache.Task.IsCompleted == false)
                        return cache.Value;
                }

                var prevValue = cache.Task.Result;
                if (prevValue == GetStatsResult.Empty)
                {
                    _previousInfo.TryRemove(new KeyValuePair<string, DiskStatsCache>(drive, cache));
                    continue;
                }

                var diff = DateTime.UtcNow - prevValue.RawSampling.Time;
                if (start < prevValue.RawSampling.Time || diff < _minInterval)
                    return prevValue.Calculated;

                state ??= new State { Drive = drive };
                state.Result = prevValue;

                var calculateTask = new Task<GetStatsResult>(CalculateStats, state);
                if (_previousInfo.TryUpdate(drive, new DiskStatsCache { Value = prevValue.Calculated, Task = calculateTask }, cache) == false)
                    continue;

                calculateTask.Start();
            }
        }

        private GetStatsResult GetStats(object o)
        {
            var state = (State)o;
            var currentInfo = GetDiskInfo(state.Drive);
            return currentInfo == null
                ? GetStatsResult.Empty
                : new GetStatsResult { RawSampling = currentInfo };
        }

        private GetStatsResult CalculateStats(object o)
        {
            var state = (State)o;
            var currentInfo = GetDiskInfo(state.Drive);
            if (currentInfo == null)
                return GetStatsResult.Empty;

            var diskSpaceResult = CalculateStats(currentInfo, state);
            return new GetStatsResult { RawSampling = currentInfo, Calculated = diskSpaceResult };
        }

        protected abstract DiskStatsResult CalculateStats(T currentInfo, State state);

        protected class State
        {
            public string Drive;
            public GetStatsResult Result;
        }

        protected abstract T GetDiskInfo(string path);

        protected class GetStatsResult
        {
            public static GetStatsResult Empty = new GetStatsResult();
            public DiskStatsResult Calculated { get; set; }
            public T RawSampling { get; set; }
        }

        private class DiskStatsCache
        {
            public DiskStatsResult Value { get; init; }
            public Task<GetStatsResult> Task { get; init; }
        }

        public abstract void Dispose();
    }
}
