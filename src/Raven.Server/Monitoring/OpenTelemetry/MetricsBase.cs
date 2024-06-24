using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Server.Config.Categories;
using Raven.Server.Monitoring.Snmp;

namespace Raven.Server.Monitoring.OpenTelemetry;

public abstract class MetricsBase(MonitoringConfiguration.OpenTelemetryConfiguration configuration)
{
    private static readonly FrozenDictionary<string, string> DescriptionMapping = SnmpOids.CreateMapping().ToFrozenDictionary();
    protected readonly MonitoringConfiguration.OpenTelemetryConfiguration Configuration = configuration;

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Id")]
    private static extern ObjectIdentifier GetOidFromObservableValue(ScalarObject oids);


    protected void CreateObservableGaugeWithTags<T, TObservable>(string name, Func<TObservable> observeValueFactory, Lazy<Meter> meter,
        string overridenDescription = null)
        where TObservable : ScalarObject, ITaggedMetricInstrument<T>
        where T : struct
    {
        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation($"{meter.Value.Name}.{name}", observableValue, overridenDescription: overridenDescription ?? GetDescription(observableValue), instrumentType: InstrumentType.Gauge);

        meter.Value.CreateObservableGauge(name: $"{meter.Value.Name}.{name}", observeValue: observableValue.GetCurrentMeasurement,
            description: overridenDescription ?? GetDescription(observableValue));
    }


    protected void CreateObservableGauge<T, TObservable>(string name, Func<TObservable> observeValueFactory, Lazy<Meter> meter)
        where TObservable : ScalarObject, IMetricInstrument<T>
        where T : struct
    {
        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation($"{meter.Value.Name}.{name}", observableValue, InstrumentType.Gauge);

        meter.Value.CreateObservableGauge(name: $"{meter.Value.Name}.{name}", observeValue: observableValue.GetCurrentMeasurement, description: GetDescription(observableValue));
    }
    
    protected void CreateObservableUpDownCounter<T, TObservable>(string name, Func<TObservable> observeValueFactory, Lazy<Meter> meter)
        where T : struct
        where TObservable : ScalarObject, IMetricInstrument<T>
    {
        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation($"{meter.Value.Name}.{name}", observableValue, InstrumentType.UpDownCounter);

        meter.Value.CreateObservableUpDownCounter(name: $"{meter.Value.Name}.{name}", observeValue: observableValue.GetCurrentMeasurement, description: GetDescription(observableValue));
    }
    
    private static string GetDescription<T>(T value)
        where T : ScalarObject
    {
        var underlyingId = GetOidFromObservableValue(value);
        ArgumentNullException.ThrowIfNull(underlyingId);

        if (DescriptionMapping.TryGetValue(underlyingId.ToString(), out var desc))
            return desc;

        ArgumentException.ThrowIfNullOrEmpty(desc);
        return null;
    }

#if DEBUG
    private static readonly ConcurrentDictionary<string, (string Description, InstrumentType instrumentType)> InstrumentDescriptionHolder = new();

    public static string GenerateTableOfInstrumentationMarkdown()
    {
        var table = new StringBuilder();
        IOrderedEnumerable<KeyValuePair<string, (string Description, InstrumentType InstrumentType)>> source = InstrumentDescriptionHolder.OrderBy(x => x.Key);
        table.AppendLine($"| Name | Description | Instrument type |");
        table.AppendLine(@"| :--- | :--- | :--- |");
        foreach (var instrument in source)
        {
            table.AppendLine($"| {instrument.Key} | {instrument.Value.Description} | {instrument.Value.InstrumentType} |");
        }

        return table.ToString();
    }
#endif

    [Conditional("DEBUG")]
    private static void RegisterInstrumentForDocumentation(string name, ScalarObject scalarObject, InstrumentType instrumentType, string overridenDescription = null)
    {
#if DEBUG
        var description = (overridenDescription ?? GetDescription(scalarObject))
            .Replace("(", @"\(")
            .Replace(")", @"\)");
        InstrumentDescriptionHolder.TryAdd(name, (overridenDescription ?? description, instrumentType));
#endif
    }

    private enum InstrumentType
    {
        Gauge,
        UpDownCounter
    }
}
