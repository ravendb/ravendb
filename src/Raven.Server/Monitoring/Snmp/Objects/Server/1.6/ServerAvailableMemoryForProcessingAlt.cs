using System;
using JetBrains.Annotations;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerAvailableMemoryForProcessingAlt : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;

        public ServerAvailableMemoryForProcessingAlt([NotNull] MetricCacher metricCacher)
            : base(SnmpOids.Server.AvailableMemoryForProcessingAlt)
        {
            _metricCacher = metricCacher ?? throw new ArgumentNullException(nameof(metricCacher));
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(_metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15SecondsAlt).AvailableMemoryForProcessing.GetValue(SizeUnit.Megabytes));
        }
    }
}
