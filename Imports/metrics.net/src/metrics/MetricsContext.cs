using System;
using Metrics.Core;
using Metrics.MetricData;
namespace Metrics
{
    /// <summary>
    /// Represents a logical grouping of metrics
    /// </summary>
    public interface MetricsContext : IDisposable, Utils.IHideObjectMembers
    {
        /// <summary>
        /// Exposes advanced operations that are possible on this metrics context.
        /// </summary>
        AdvancedMetricsContext Advanced { get; }

        /// <summary>
        /// Create a new child metrics context. Metrics added to the child context are kept separate from the metrics in the 
        /// parent context.
        /// </summary>
        /// <param name="contextName">Name of the child context.</param>
        /// <param name="contextCreator">Function used to create the instance of the child context. (Use for creating custom contexts)</param>
        /// <returns>Newly created child context.</returns>
        MetricsContext Context(string contextName, Func<string, MetricsContext> contextCreator);

        /// <summary>
        /// Remove a child context. The metrics for the child context are removed from the MetricsData of the parent context.
        /// </summary>
        /// <param name="contextName">Name of the child context to shutdown.</param>
        void ShutdownContext(string contextName);
        
        /// <summary>
        /// A meter measures the rate at which a set of events occur, in a few different ways. 
        /// This metric is suitable for keeping a record of now often something happens ( error, request etc ).
        /// </summary>
        /// <remarks>
        /// The mean rate is the average rate of events. It’s generally useful for trivia, 
        /// but as it represents the total rate for your application’s entire lifetime (e.g., the total number of requests handled, 
        /// divided by the number of seconds the process has been running), it doesn’t offer a sense of recency. 
        /// Luckily, meters also record three different exponentially-weighted moving average rates: the 1-, 5-, and 15-minute moving averages.
        /// </remarks>
        /// <param name="name">Name of the metric. Must be unique across all meters in this context.</param>
        /// <returns>Reference to the metric</returns>
        Meter Meter(string name);

        /// <summary>
        /// A Histogram measures the distribution of values in a stream of data: e.g., the number of results returned by a search.
        /// </summary>
        /// <param name="name">Name of the metric. Must be unique across all histograms in this context.</param>
        /// <returns>Reference to the metric</returns>
        Histogram Histogram(string name);

        Meter PerSecondMetric(string name);

        Meter BufferedAverageMeter(string name, int bufferSize = 10, int intervalInSeconds = 1);
    }
}
