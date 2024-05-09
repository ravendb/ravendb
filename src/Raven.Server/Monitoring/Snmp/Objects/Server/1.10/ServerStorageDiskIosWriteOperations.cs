using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class ServerStorageDiskIosWriteOperations : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        private readonly ServerStore _store;
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerStorageDiskIosWriteOperations(ServerStore store, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.StorageDiskIoWriteOperations)
        {
            _store = store;
            _nodeTag = nodeTag;
        }

        private int? Value
        {
            get
            {
                if (_store.Configuration.Core.RunInMemory)
                    return null;

                var result = _store.Server.DiskStatsGetter.Get(_store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
                return result == null ? null : (int)Math.Round(result.IoWriteOperations);
            }
        }

        protected override Gauge32 GetData()
        {
            var result = Value;
            return result == null ? null : new Gauge32(result.Value);
        }

        public Measurement<int> GetCurrentValue()
        {
            return new(Value ?? -1, _nodeTag);
        }
    }
}
