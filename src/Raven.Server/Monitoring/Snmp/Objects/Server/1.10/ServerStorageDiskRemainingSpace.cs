using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerStorageDiskRemainingSpace : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly ServerStore _store;
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerStorageDiskRemainingSpace(ServerStore store, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.StorageDiskRemainingSpace)
        {
            _store = store;
            _nodeTag = nodeTag;
        }

        private long? Value
        {
            get
            {
                if (_store.Configuration.Core.RunInMemory)
                    return null;
                
                var result = _store.Server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
                if (result == null)
                    return null;

                return result.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
            }
        }
        
        protected override Gauge32 GetData()
        {
            var current = Value;
            return current.HasValue 
                ? new Gauge32(current.Value) 
                : null;
        }

        public Measurement<long> GetCurrentValue()
        {
            return new Measurement<long>(Value ?? -1, _nodeTag);
        }
    }
}
