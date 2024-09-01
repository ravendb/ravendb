using System.Diagnostics.Metrics;

namespace Raven.Server.Monitoring.OpenTelemetry;

public interface IMetricInstrument<out TInstrumentValue>
{
    TInstrumentValue GetCurrentMeasurement();
}

public interface ITaggedMetricInstrument<TInstrumentValue> : IMetricInstrument<Measurement<TInstrumentValue>>
    where TInstrumentValue : struct
{
}
