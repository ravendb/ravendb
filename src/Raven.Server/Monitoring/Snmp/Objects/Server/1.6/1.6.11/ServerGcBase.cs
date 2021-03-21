using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public abstract class ServerGcBase<TData> : ScalarObjectBase<TData>
        where TData : ISnmpData
    {
        private readonly MetricCacher _metricCacher;
        private readonly string _cacheKey;

        protected ServerGcBase(MetricCacher metricCacher, GCKind gcKind, string dots)
            : base(dots, (int)gcKind)
        {
            _metricCacher = metricCacher;
            _cacheKey = GetCacheKey(gcKind);
        }

        private static string GetCacheKey(GCKind gcKind)
        {
            switch (gcKind)
            {
                case GCKind.Any:
                    return MetricCacher.Keys.Server.GcAny;

                case GCKind.Ephemeral:
                    return MetricCacher.Keys.Server.GcEphemeral;

                case GCKind.FullBlocking:
                    return MetricCacher.Keys.Server.GcFullBlocking;

                case GCKind.Background:
                    return MetricCacher.Keys.Server.GcBackground;

                default:
                    throw new ArgumentOutOfRangeException(nameof(gcKind));
            }
        }

        protected GCMemoryInfo GetGCMemoryInfo()
        {
            return _metricCacher.GetValue<GCMemoryInfo>(_cacheKey);
        }
    }
}
