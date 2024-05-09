using System;
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
    public sealed class ServerStorageDiskRemainingSpacePercentage : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        private readonly ServerStore _store;
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerStorageDiskRemainingSpacePercentage(ServerStore store, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.StorageDiskRemainingSpacePercentage)
        {
            _store = store;
            _nodeTag = nodeTag;
        }

        public int? Value
        {
            get
            {
                if (_store.Configuration.Core.RunInMemory)
                    return null;

                var result = _store.Server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
                if (result == null)
                    return null;

                var total = Convert.ToDecimal(result.TotalSize.GetValue(SizeUnit.Megabytes));
                var totalFree = Convert.ToDecimal(result.TotalFreeSpace.GetValue(SizeUnit.Megabytes));
                return Convert.ToInt32(Math.Round((totalFree / total) * 100, 0, MidpointRounding.ToEven));
            }
        }

        protected override Gauge32 GetData()
        {
            var percentage = Value;
            return percentage.HasValue 
                ? new Gauge32(percentage.Value) 
                : null;
        }

        public Measurement<int> GetCurrentValue()
        {
            return new(Value ?? -1, _nodeTag);
        }
    }
}
