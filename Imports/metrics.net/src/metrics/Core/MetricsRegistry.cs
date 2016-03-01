
using System;
using System.Collections.Generic;
using Metrics.MetricData;
namespace Metrics.Core
{
    public interface RegistryDataProvider
    {
        IEnumerable<Meter> Meters { get; }
        IEnumerable<Histogram> Histograms { get; }
    }

    public interface MetricsRegistry
    {
        RegistryDataProvider DataProvider { get; }

        Meter Meter(string name, Func<string,Meter> builder);

        Histogram Histogram(string name, Func<string,Histogram> builder);

        Meter PerSecondMetric(string name, Func<string, Meter> builder);

        void ClearAllMetrics();
        void ResetMetricsValues();
        Meter BufferedAverageMeter(string name, Func<object, Meter> func);
    }
}
