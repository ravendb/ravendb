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
    public sealed class ServerAvailableMemoryForProcessingPercentage : ScalarObjectBase<Gauge32>, IMetricInstrument<int>
    {
        private readonly MetricCacher _metricCacher;

        public ServerAvailableMemoryForProcessingPercentage([NotNull] MetricCacher metricCacher)
            : base(SnmpOids.Server.AvailableMemoryForProcessingPercentage)
        {
            _metricCacher = metricCacher ?? throw new ArgumentNullException(nameof(metricCacher));
        }

        private int? Value
        {
            get
            {
                var mem = _metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds);
                
                var totalMb = Convert.ToDecimal(mem.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes));
                if (totalMb <= 0)
                    return null;

                var availableMb = Convert.ToDecimal(mem.AvailableMemoryForProcessing.GetValue(SizeUnit.Megabytes));
                var pct =  Convert.ToInt32(Math.Round((availableMb / totalMb) * 100, 0, MidpointRounding.ToEven));
                
                return Math.Max(0, pct);
            }
        }

        protected override Gauge32 GetData()
        {
            var percentage = Value;
            return percentage.HasValue ? new Gauge32(percentage.Value) : null;
        }

        public int GetCurrentMeasurement() => Value ?? -1;
    }
}
