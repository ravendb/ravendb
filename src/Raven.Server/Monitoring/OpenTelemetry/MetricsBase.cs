using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using Raven.Server.Config.Categories;
using Raven.Server.Rachis;

namespace Raven.Server.Monitoring.OpenTelemetry;

public abstract class MetricsBase
{
    private static string _nodeTag = null;
    public static string NodeTag
    {
        get
        {
            if (_nodeTag == null || _nodeTag == RachisConsensus.InitialTag)
                throw new InvalidOperationException(
                    $"{nameof(NodeTag)} is neither 'null' nor default. OpenTelemetry in such case should not be running. This indicates a bug!");
            
            return _nodeTag;
        }
        set
        {
            if (_nodeTag != null)
                throw new InvalidDataException($"{nameof(NodeTag)} for metrics is already set. This is a bug");

            _nodeTag = value;
        }
    }

    protected readonly MonitoringConfiguration.OpenTelemetryConfiguration Configuration;
    protected MetricsBase(MonitoringConfiguration.OpenTelemetryConfiguration configuration)
    {
        Configuration = configuration;
    }
    
    protected void CreateObservableGaugeWithTags<T, TObservable>(string name, TObservable observeValue, string description, string[] family, Lazy<Meter> meter)
        where TObservable : ITaggedMetricInstrument<T>
        where T : struct
    {
        if (family != null && family.Contains(name) == false)
            return;

        var x = meter.Value.CreateObservableGauge(name: name, observeValue: observeValue.GetCurrentValue, description: description);
    }
    
    protected void CreateObservableUpDownCounterWithTags<T, TObservable>(string name, TObservable observeValue, string description, string[] family, Lazy<Meter> meter)
        where T : struct
        where TObservable : ITaggedMetricInstrument<T>
    {
        if (family != null && family.Contains(name) == false) 
            return;

        meter.Value.CreateObservableUpDownCounter(name: name, observeValue: observeValue.GetCurrentValue, description: description);
    }
}
