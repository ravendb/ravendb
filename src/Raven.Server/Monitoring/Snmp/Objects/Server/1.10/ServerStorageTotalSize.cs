using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerStorageTotalSize(ServerStore store) : ScalarObjectBase<Gauge32>(SnmpOids.Server.StorageTotalSize), IMetricInstrument<long>
    {
        private long Value
        {
            get
            {
                var size = store._env.Stats().AllocatedDataFileSizeInBytes;
                return size / 1024L / 1024L;
            }
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }
        
        public long GetCurrentMeasurement() => Value;
    }
}
