

namespace Metrics.MetricData
{
    /// <summary>
    /// A provider capable of returning the current values for a set of metrics
    /// </summary>
    public interface MetricsDataProvider : Utils.IHideObjectMembers
    {
        /// <summary>
        /// Returns the current metrics data for the context for which this provider has been created.
        /// </summary>
        MetricsData CurrentMetricsData { get; }
    }
  
}
