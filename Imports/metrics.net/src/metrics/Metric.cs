using System;
using System.Diagnostics;

namespace Metrics
{
    /// <summary>
    /// Static wrapper around a global MetricContext instance.
    /// </summary>
    public static class Metric
    {

        private static readonly DefaultMetricsContext globalContext;

        static Metric()
        {
            globalContext = new DefaultMetricsContext(GetGlobalContextName());
        }

        /// <summary>
        /// Exposes advanced operations that are possible on this metrics context.
        /// </summary>
        public static AdvancedMetricsContext Advanced { get { return globalContext; } }

        /// <summary>
        /// Create a new child metrics context. Metrics added to the child context are kept separate from the metrics in the 
        /// parent context.
        /// </summary>
        /// <param name="contextName">Name of the child context.</param>
        /// <returns>Newly created child context.</returns>
        public static MetricsContext Context(string contextName)
        {
            return globalContext.Context(contextName);
        }

        /// <summary>
        /// Create a new child metrics context. Metrics added to the child context are kept separate from the metrics in the 
        /// parent context.
        /// </summary>
        /// <param name="contextName">Name of the child context.</param>
        /// <param name="contextCreator">Function used to create the instance of the child context. (Use for creating custom contexts)</param>
        /// <returns>Newly created child context.</returns>
        public static MetricsContext Context(string contextName, Func<string, MetricsContext> contextCreator)
        {
            return globalContext.Context(contextName, contextCreator);
        }

        /// <summary>
        /// Remove a child context. The metrics for the child context are removed from the MetricsData of the parent context.
        /// </summary>
        /// <param name="contextName">Name of the child context to shutdown.</param>
        public static void ShutdownContext(string contextName)
        {
            globalContext.ShutdownContext(contextName);
        }

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
        public static Meter Meter(string name)
        {
            return globalContext.Meter(name);
        }
        

        /// <summary>
        /// A Histogram measures the distribution of values in a stream of data: e.g., the number of results returned by a search.
        /// </summary>
        /// <param name="name">Name of the metric. Must be unique across all histograms in this context.</param>
        /// <returns>Reference to the metric</returns>
        public static Histogram Histogram(string name)
        {
            return globalContext.Histogram(name);
        }

        private static string GetGlobalContextName()
        {
            try
            {

                var name = Process.GetCurrentProcess().ProcessName.Replace('.', '_');
                return name;
            }
            catch (Exception x)
            {
                throw new InvalidOperationException("Invalid Metrics Configuration: Metrics.GlobalContextName must be non empty string", x);
            }
        }
    }
}
