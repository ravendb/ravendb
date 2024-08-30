using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerUnmanagedMemory() : ScalarObjectBase<Gauge32>(SnmpOids.Server.UnmanagedMemory), IMetricInstrument<long>
    {
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

        public long GetCurrentMeasurement() => Value;
    }
}
