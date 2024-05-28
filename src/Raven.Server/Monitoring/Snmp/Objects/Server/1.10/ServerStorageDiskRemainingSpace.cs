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
    public sealed class ServerStorageDiskRemainingSpace(ServerStore store) : ScalarObjectBase<Gauge32>(SnmpOids.Server.StorageDiskRemainingSpace), IMetricInstrument<long>
    {
        private long? Value
        {
            get
            {
                if (store.Configuration.Core.RunInMemory)
                    return null;
                
                var result = store.Server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
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

        public long GetCurrentMeasurement() => Value ?? -1;
    }
}
