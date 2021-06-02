using System;
using System.Collections.Concurrent;
using System.Threading;
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

                public const string MemoryInfoExtended = "MemoryInfoExtended";

                public const string DiskSpaceInfo = "DiskSpaceInfo";

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

        private readonly ConcurrentDictionary<string, MetricValueBase> _metrics = new ConcurrentDictionary<string, MetricValueBase>(StringComparer.OrdinalIgnoreCase);

        public void Register<T>(string key, TimeSpan refreshRate, Func<T> factory)
        {
            if (_metrics.TryAdd(key, new MetricValue<T>(refreshRate, factory)) == false)
                throw new InvalidOperationException($"Cannot cache '{key}' metric, because it already exists.");
        }

        public T GetValue<T>(string key, Func<T> factory = null)
        {
            if (_metrics.TryGetValue(key, out var value) == false)
                throw new InvalidOperationException($"Metric '{key}' was not found.");

            if (value.ShouldRefresh(out var first) == false)
                return (T)value.Value;

            if (first)
            {
                lock (value)
                {
                    if (value.ShouldRefresh(out first))
                        value.Refresh();

                    return (T)value.Value;
                }
            }

            if (Monitor.TryEnter(value, 0) == false)
                return (T)value.Value;

            try
            {
                if (value.ShouldRefresh(out first))
                    value.Refresh();

                return (T)value.Value;
            }
            finally
            {
                Monitor.Exit(value);
            }
        }

        private class MetricValue<T> : MetricValueBase
        {
            private readonly Func<T> _factory;

            public MetricValue(TimeSpan refreshRate, Func<T> factory)
                : base(refreshRate)
            {
                if (factory == null)
                    throw new ArgumentNullException(nameof(factory));

                _factory = factory;
            }

            protected override object RefreshInternal()
            {
                return _factory();
            }
        }

        private abstract class MetricValueBase
        {
            private DateTime _lastRefresh;
            private TimeSpan _refreshRate;

            protected MetricValueBase(TimeSpan refreshRate)
            {
                _refreshRate = refreshRate;
            }

            public object Value { get; private set; }

            protected abstract object RefreshInternal();

            internal void Refresh()
            {
                try
                {
                    Value = RefreshInternal();
                }
                finally
                {
                    _lastRefresh = SystemTime.UtcNow;
                }
            }

            internal bool ShouldRefresh(out bool first)
            {
                first = _lastRefresh == DateTime.MinValue;

                return first || SystemTime.UtcNow - _lastRefresh >= _refreshRate;
            }
        }
    }
}
