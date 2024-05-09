using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Sparrow.Server.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerDirtyMemory : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerDirtyMemory(KeyValuePair<string, object> nodeTag = default) : base(SnmpOids.Server.DirtyMemory)
        {
            _nodeTag = nodeTag;
        }
        
        private long Value => MemoryInformation.GetDirtyMemoryState().TotalDirty.GetValue(SizeUnit.Megabytes);

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
