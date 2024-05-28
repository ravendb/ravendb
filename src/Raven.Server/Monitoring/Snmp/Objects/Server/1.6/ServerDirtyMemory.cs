using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;
using Sparrow.Server.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerDirtyMemory() : ScalarObjectBase<Gauge32>(SnmpOids.Server.DirtyMemory), IMetricInstrument<long>
    {
        private long Value => MemoryInformation.GetDirtyMemoryState().TotalDirty.GetValue(SizeUnit.Megabytes);

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}
