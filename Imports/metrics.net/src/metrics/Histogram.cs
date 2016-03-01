
namespace Metrics
{
    /// <summary>
    /// A Histogram measures the distribution of values in a stream of data: e.g., the number of results returned by a search.
    /// </summary>
    public interface Histogram : ResetableMetric
    {
        /// <summary>
        /// Records a value.
        /// </summary>
        /// <param name="value">Value to be added to the histogram.</param>
        /// Useful for tracking (for example) for which id the max or min value was recorded.
        void Update(long value);
    }
}
