using System;
using Metrics.Core;
using Metrics.MetricData;
using Metrics.Sampling;

namespace Metrics
{
    public interface AdvancedMetricsContext : Utils.IHideObjectMembers
    {
        /// <summary>
        /// All metrics operations will be NO-OP.
        /// This is useful for measuring the impact of the metrics library on the application.
        /// If you think the Metrics library is causing issues, this will disable all Metrics operations.
        /// </summary>
        void CompletelyDisableMetrics();

        /// <summary>
        /// Clear all collected data for all the metrics in this context
        /// </summary>
        void ResetMetricsValues();

        /// <summary>
        /// Event fired when the context is disposed or shutdown or the CompletelyDisableMetrics is called.
        /// </summary>
        event EventHandler ContextShuttingDown;

        /// <summary>
        /// Event fired when the context CompletelyDisableMetrics is called.
        /// </summary>
        event EventHandler ContextDisabled;

        /// <summary>
        /// Register a custom Meter instance.
        /// </summary>
        /// <param name="name">Name of the metric. Must be unique across all meters in this context.</param>
        /// <returns>Reference to the metric</returns>
        Meter Meter(string name);

        /// <summary>
        /// Register a custom Histogram instance
        /// </summary>
        /// <param name="name">Name of the metric. Must be unique across all histograms in this context.</param>
        /// <returns>Reference to the metric</returns>
        Histogram Histogram(string name);

    }
}
