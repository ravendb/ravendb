using System;
using JetBrains.Annotations;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerAvailableMemoryForProcessing : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;

        public ServerAvailableMemoryForProcessing([NotNull] MetricCacher metricCacher)
            : base(SnmpOids.Server.AvailableMemoryForProcessing)
        {
            _metricCacher = metricCacher ?? throw new ArgumentNullException(nameof(metricCacher));
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(_metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).AvailableMemoryForProcessing.GetValue(SizeUnit.Megabytes));
        }
    }
}
