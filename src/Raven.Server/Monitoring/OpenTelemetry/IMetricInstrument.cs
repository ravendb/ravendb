using System.Diagnostics.Metrics;

namespace Raven.Server.Monitoring.OpenTelemetry;

public interface PlaceHolderReflection{}

public interface IMetricInstrument<out TInstrumentValue>: PlaceHolderReflection
{
    TInstrumentValue GetCurrentMeasurement();
}

public interface ITaggedMetricInstrument<TInstrumentValue> : IMetricInstrument<Measurement<TInstrumentValue>>
    where TInstrumentValue : struct
{
}
