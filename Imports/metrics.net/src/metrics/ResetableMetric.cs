
namespace Metrics
{
    /// <summary>
    /// Indicates a metric's ability to be reset. Reseting a metric clear all currently collected data.
    /// </summary>
    public interface ResetableMetric : Utils.IHideObjectMembers
    {
        string Name { get; }
        /// <summary>
        /// Clear all currently collected data for this metric.
        /// </summary>
        void Reset();
    }
}
