using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerUnmanagedMemory : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerUnmanagedMemory(KeyValuePair<string, object> nodeTag  = default) : base(SnmpOids.Server.UnmanagedMemory)
        {
            _nodeTag = nodeTag;
        }

        private long Value
        {
            get
            {
                var unmanagedMemoryInBytes = AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes();
                return new Size(unmanagedMemoryInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
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
