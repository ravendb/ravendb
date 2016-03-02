
namespace Metrics
{
    /// <summary>
    /// A meter measures the rate at which a set of events occur, in a few different ways. 
    /// The mean rate is the average rate of events. It’s generally useful for trivia, 
    /// but as it represents the total rate for your application’s entire lifetime (e.g., the total number of requests handled, 
    /// divided by the number of seconds the process has been running), it doesn’t offer a sense of recency. 
    /// Luckily, meters also record three different exponentially-weighted moving average rates: the 1-, 5-, and 15-minute moving averages.
    /// </summary>
    public interface Meter : ResetableMetric, Utils.IHideObjectMembers
    {
        /// <summary>
        /// Mark the occurrence of <paramref name="count"/> events.
        /// </summary>
        /// <param name="count"></param>
        void Mark(long count=1);
    }
}
