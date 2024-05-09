using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerStorageUsedSize : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly ServerStore _store;
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerStorageUsedSize(ServerStore store, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.StorageUsedSize)
        {
            _store = store;
            _nodeTag = nodeTag;
        }

        private long Value
        {
            get
            {
                var stats = _store._env.Stats();
                return stats.UsedDataFileSizeInBytes / 1024L / 1024L;
            }
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public Measurement<long> GetCurrentValue()
        {
            return new(Value, _nodeTag);
        }
    }
}
