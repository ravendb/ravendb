using System.Collections.Generic;
using metrics.Core;

namespace metrics.Util
{
    internal static class Utils
    {
        internal static IDictionary<string, IDictionary<string, IMetric>> SortMetrics(IDictionary<MetricName, IMetric> metrics)
        {
            var sortedMetrics = new SortedDictionary<string, IDictionary<string, IMetric>>();

            foreach(var entry in metrics)
            {
                var fullName = entry.Key.Context;
                IDictionary<string, IMetric> submetrics;
                if(!sortedMetrics.ContainsKey(fullName))
                {
                    submetrics = new SortedDictionary<string, IMetric>();
                    sortedMetrics.Add(fullName, submetrics);
                }
                else
                {
                    submetrics = sortedMetrics[fullName];
                }
                submetrics.Add(entry.Key.Name, entry.Value);
            }
            return sortedMetrics;
        }
    }
}
