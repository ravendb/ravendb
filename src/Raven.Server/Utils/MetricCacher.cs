using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils
{
    public class MetricCacher
    {
        public sealed class Keys
        {
            private Keys()
            {
            }

            public sealed class Server
            {
                private Server()
                {
                }


                public const string CpuUsage = "CpuUsage";

                public const string MemoryInfo = "MemoryInfo";

                public sealed class MemoryInfoExtended
                {
                    public const string RefreshRate15Seconds = "MemoryInfoExtended/RefreshRate15Seconds";

                    public const string RefreshRate5Seconds = "MemoryInfoExtended/RefreshRate5Seconds";
                }

                public const string DiskSpaceInfo = "DiskSpaceInfo";

                public const string MemInfo = "MemInfo";

                public const string MaxServerLimits = "MaxServerLimits";

                public const string CurrentServerLimits = "CurrentServerLimits";

                public const string GcAny = "GC/Any";

                public const string GcEphemeral = "GC/Ephemeral";

                public const string GcFullBlocking = "GC/FullBlocking";

                public const string GcBackground = "GC/Background";
            }

            public sealed class Database
            {
                private Database()
                {
                }

                public const string DiskSpaceInfo = "DiskSpaceInfo";
            }
        }

        private readonly ConcurrentDictionary<string, MetricValue> _metrics = new(StringComparer.OrdinalIgnoreCase);

        public void Register(string key, TimeSpan refreshRate, Func<object> factory, bool asyncRefresh = true)
        {
            if (_metrics.TryAdd(key, new MetricValue(refreshRate, key, factory, asyncRefresh)) == false)
                throw new InvalidOperationException($"Cannot cache '{key}' metric, because it already exists.");
        }

        public T GetValue<T>(string key, Func<T> factory = null)
        {
            if (_metrics.TryGetValue(key, out var value) == false)
                throw new InvalidOperationException($"Metric '{key}' was not found.");

            var result = value.GetRefreshedValue();
            if (result == null)
                return default;
            return (T)result;
        }

        private sealed class MetricValue
        {
            private readonly TimeSpan _refreshRate;
            private readonly Func<object> _factory;
            private readonly bool _asyncRefresh;
            private Task<DateTime> _task;
            private object _value;
            private long _observedFailureTicks;
            private readonly RavenLogger _logger;

            public MetricValue(TimeSpan refreshRate, string key, Func<object> factory, bool asyncRefresh = true)
            {
                _logger = RavenLogManager.Instance.GetLoggerForServer<MetricValue>(LoggingComponent.Name(key));
                _refreshRate = refreshRate;
                _factory = factory;
                _asyncRefresh = asyncRefresh;
                _task = Task.FromResult(SystemTime.UtcNow + _refreshRate);
                try
                {
                    _value = factory();
                }
                catch (Exception e)
                {
                    _value = default;
                    if (_logger.IsWarnEnabled)
                    {
                        _logger.Warn("Got an error while refreshing value", e);
                    }
                }
            }

            public object GetRefreshedValue()
            {
                var currentTask = _task;
                if (currentTask.IsCompleted == false)
                {
                    if (_asyncRefresh == false)
                    {
                        // don't want to return stale value
                        // let's wait until current value calculation is done

                        currentTask.Wait();
                    }

                    return _value; // return current value, while it is being computed
                }

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

                    return _value; // return cached value in case of an error, better than throwing.
                }

                var nextRefreshForTask = currentTask.Result;
                if (SystemTime.UtcNow < nextRefreshForTask)
                    return _value;// no need to refresh yet...

                TryStartingRefreshTask();

                if (_asyncRefresh)
                {
                    // we may have started the task (or another thread did), but we don't know if we got a new value or not
                    // we don't care, we are okay with getting the "old" value here, since it will update
                    // soon, and this is likely called many times over, so as long as we get it to some point, we are good
                    return _value;
                }

                // if we don't want to return "old" value we have the option to force waiting
                // for the new value calculated by just started task

                currentTask = _task;

                if (currentTask.IsCompleted == false)
                    currentTask.Wait();

                return _value;

                void TryStartingRefreshTask()
                {
                    var task = new Task<DateTime>(() =>
                    {
                        object result;
                        try
                        {
                            result = _factory();
                        }
                        catch (Exception e)
                        {
                            if (_logger.IsWarnEnabled)
                            {
                                _logger.Warn("Got an error while refreshing value", e);
                            }
                            throw;
                        }
                        var nextRefresh = SystemTime.UtcNow + _refreshRate;
                        Interlocked.Exchange(ref _value, result);
                        if (currentTask.IsFaulted)
                        {
                            if (_logger.IsWarnEnabled)
                            {
                                _logger.Warn("Recovered from error in refreshing value.");
                            }
                        }
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
