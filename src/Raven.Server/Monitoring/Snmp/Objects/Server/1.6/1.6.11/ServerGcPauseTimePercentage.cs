using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerGcPauseTimePercentage : ServerGcBase<Gauge32>, ITaggedMetricInstrument<int>
{
    public ServerGcPauseTimePercentage(MetricCacher metricCacher, GCKind gcKind)
        : base(metricCacher, gcKind, SnmpOids.Server.GcPauseTimePercentage)
    {
    }

    private int Value
    {
        get
        {
            var pauseTimePercentage = GetGCMemoryInfo().PauseTimePercentage;
            return (int)pauseTimePercentage;
        }
    }
        
    protected override Gauge32 GetData()
    {
        return new Gauge32(Value);
    }

    public Measurement<int> GetCurrentMeasurement() => new(Value, MeasurementTag);
}
