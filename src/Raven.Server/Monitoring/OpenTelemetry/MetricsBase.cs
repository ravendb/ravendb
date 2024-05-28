using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
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

    
    protected void CreateObservableGaugeWithTags<T, TObservable>(string name, Func<TObservable> observeValueFactory,
        Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family, Lazy<Meter> meter, string overridenDescription = null)
        where TObservable : ScalarObject, ITaggedMetricInstrument<T>
        where T : struct
    {
        if (ShouldSkipInstrument(name, family))
            return;

        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation(name, family, observableValue);

        meter.Value.CreateObservableGauge(name: name, observeValue: observableValue.GetCurrentMeasurement, description: overridenDescription ?? GetDescription(observableValue));
    }



    protected void CreateObservableGauge<T, TObservable>(string name, Func<TObservable> observeValueFactory,
        Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family, Lazy<Meter> meter)
        where TObservable : ScalarObject, IMetricInstrument<T>
        where T : struct
    {
        if (ShouldSkipInstrument(name, family))
            return;

        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation(name, family, observableValue);

        meter.Value.CreateObservableGauge(name: name, observeValue: observableValue.GetCurrentMeasurement, description: GetDescription(observableValue));
    }

    protected void CreateObservableUpDownCounterWithTags<T, TObservable>(string name, Func<TObservable> observeValueFactory,
        Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family, Lazy<Meter> meter)
        where T : struct
        where TObservable : ScalarObject, ITaggedMetricInstrument<T>
    {
        if (ShouldSkipInstrument(name, family))
            return;

        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation(name, family, observableValue);

        meter.Value.CreateObservableUpDownCounter(name: name, observeValue: observableValue.GetCurrentMeasurement, description: GetDescription(observableValue));
    }

    protected void CreateObservableUpDownCounter<T, TObservable>(string name, Func<TObservable> observeValueFactory,
        Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family, Lazy<Meter> meter)
        where T : struct
        where TObservable : ScalarObject, IMetricInstrument<T>
    {
        if (ShouldSkipInstrument(name, family))
            return;

        var observableValue = observeValueFactory.Invoke();
        RegisterInstrumentForDocumentation(name, family, observableValue);
        
        meter.Value.CreateObservableUpDownCounter(name: name, observeValue: observableValue.GetCurrentMeasurement, description: GetDescription(observableValue));
    }

    private bool ShouldSkipInstrument(string name, Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family)
    {
        var filter = family?.Compile()?.Invoke(Configuration);
        return filter != null && filter.Contains(name) == false;
    }

    private static string GetDescription<T>(T value)
        where T : ScalarObject
    {
        var underlyingId = GetOidFromObservableValue(value);
        ArgumentNullException.ThrowIfNull(underlyingId);
        
        var desc = DescriptionMapping[underlyingId.ToString()];
        ArgumentException.ThrowIfNullOrEmpty(desc);
       
        return desc;
    }
    
#if DEBUG
    public static readonly ConcurrentDictionary<(string Name, string Family), string> InstrumentDescriptionHolder = new();

    public static string GenerateTableOfInstrumentationMarkdown()
    {
        var table = new StringBuilder();
        var source = InstrumentDescriptionHolder.OrderBy(x => x.Key.Family).ThenBy(x => x.Key.Name);
        table.AppendLine($"| Family | Name | Description |");
        table.AppendLine(@"| :--- | :--- | :--- |");
        foreach (var instrument in source)
        {
            table.AppendLine($"| {instrument.Key.Family} | {instrument.Key.Name} | {instrument.Value} |");
        }

        return table.ToString();
    }
    
#endif

    [Conditional("DEBUG")]
    private static void RegisterInstrumentForDocumentation(string name, Expression<Func<MonitoringConfiguration.OpenTelemetryConfiguration, string[]>> family, ScalarObject scalarObject, string overridenDescription = null)
    {
#if DEBUG
        var familyName = family.Body.ToString().Split('.')[1];
        var description = (overridenDescription ?? GetDescription(scalarObject))
            .Replace("(", @"\(")
            .Replace(")", @"\)");
        InstrumentDescriptionHolder.TryAdd((Name: name, Family: familyName), description);
#endif
    }
}
