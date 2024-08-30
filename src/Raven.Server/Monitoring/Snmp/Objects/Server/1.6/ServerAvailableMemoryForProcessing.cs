using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using JetBrains.Annotations;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerAvailableMemoryForProcessing : ScalarObjectBase<Gauge32>, IMetricInstrument<long>
    {
        private readonly MetricCacher _metricCacher;
        
        public ServerAvailableMemoryForProcessing([NotNull] MetricCacher metricCacher)
            : base(SnmpOids.Server.AvailableMemoryForProcessing)
        {
            _metricCacher = metricCacher ?? throw new ArgumentNullException(nameof(metricCacher));

        }

        private long Value => _metricCacher
            .GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).AvailableMemoryForProcessing
            .GetValue(SizeUnit.Megabytes);

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}
