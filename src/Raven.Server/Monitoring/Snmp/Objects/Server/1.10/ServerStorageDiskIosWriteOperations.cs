using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class ServerStorageDiskIosWriteOperations(ServerStore store)
        : ScalarObjectBase<Gauge32>(SnmpOids.Server.StorageDiskIoWriteOperations), IMetricInstrument<int>
    {
        private int? Value
        {
            get
            {
                if (store.Configuration.Core.RunInMemory)
                    return null;

                var result = store.Server.DiskStatsGetter.Get(store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
                return result == null ? null : (int)Math.Round(result.IoWriteOperations);
            }
        }

        protected override Gauge32 GetData()
        {
            var result = Value;
            return result == null ? null : new Gauge32(result.Value);
        }

        public int GetCurrentMeasurement() => Value ?? -1;
    }
}
