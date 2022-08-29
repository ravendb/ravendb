using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;

namespace Raven.Server.Utils
{
    public class MetricCacher
    {
        public class Keys
        {
            private Keys()
            {
            }

            public class Server
            {
                private Server()
                {
                }


                public const string CpuUsage = "CpuUsage";

                public const string MemoryInfo = "MemoryInfo";

                public class MemoryInfoExtended
                {
                    public const string RefreshRate15Seconds = "MemoryInfoExtended/RefreshRate15Seconds";

                    public const string RefreshRate5Seconds = "MemoryInfoExtended/RefreshRate5Seconds";
                }

                public const string DiskSpaceInfo = "DiskSpaceInfo";

                public const string MemInfo = "MemInfo";

                public const string GcAny = "GC/Any";

                public const string GcEphemeral = "GC/Ephemeral";

                public const string GcFullBlocking = "GC/FullBlocking";

                public const string GcBackground = "GC/Background";
            }

            public class Database
            {
                private Database()
                {
                }

                public const string DiskSpaceInfo = "DiskSpaceInfo";
            }
        }

        private readonly ConcurrentDictionary<string, MetricValue> _metrics = new(StringComparer.OrdinalIgnoreCase);

        public void Register(string key, TimeSpan refreshRate, Func<object> factory)
        {
            if (_metrics.TryAdd(key, new MetricValue(refreshRate, factory)) == false)
                throw new InvalidOperationException($"Cannot cache '{key}' metric, because it already exists.");
        }

        public T GetValue<T>(string key, Func<T> factory = null)
        {
            if (_metrics.TryGetValue(key, out var value) == false)
                throw new InvalidOperationException($"Metric '{key}' was not found.");

            return (T)value.GetRefreshedValue();
        }

        private class MetricValue
        {
            private readonly TimeSpan _refreshRate;
            private readonly Func<object> _factory;
            private Task<DateTime> _task;
            private object _value;
            private long _observedFailureTicks;

            public MetricValue(TimeSpan refreshRate, Func<object> factory)
            {
                _refreshRate = refreshRate;
                _factory = factory;
                _value = factory();
                _task = Task.FromResult(SystemTime.UtcNow + _refreshRate);
            }

            public object GetRefreshedValue()
            {
                var currentTask = _task;
                if (currentTask.IsCompleted == false)
                    return _value; // return current value, while it is being computed

                if (currentTask.IsFaulted)
                {
                    // if we failed, we'll retry
                    var failureAt= new DateTime(_observedFailureTicks);
                    if (SystemTime.UtcNow - failureAt > _refreshRate)
                    {
                        // but only at the same rate as we are expected too
                        Interlocked.Exchange(ref _observedFailureTicks, SystemTime.UtcNow.Ticks);
                        TryStartingRefreshTask();
                    }
                    // the error must be reported 
                    throw new InvalidOperationException("Failed to get value", currentTask.Exception);
                }

                var nextRefreshForTask = currentTask.Result;
                if (SystemTime.UtcNow < nextRefreshForTask)
                    return _value;// no need to refresh yet...

                TryStartingRefreshTask();
                
                // we may have started the task (or another thread did), but we don't know if we got a new value or not
                // we don't care, we are okay with getting the "old" value here, since it will update
                // soon, and this is likely called many times over, so as long as we get it to some point, we are good
                return _value;

                void TryStartingRefreshTask()
                {
                    var task = new Task<DateTime>(() =>
                    {
                        var result = _factory();
                        var nextRefresh = SystemTime.UtcNow + _refreshRate;
                        Interlocked.Exchange(ref _value, result);
                        return nextRefresh;
                    });
                    // try to update the task
                    if (Interlocked.CompareExchange(ref _task, task, currentTask) == currentTask)
                    {
                        // we won the right to start the task
                        task.Start();
                    }
                }
            }
        }
    }
}
