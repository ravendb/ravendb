using Raven.Abstractions.Replication;

namespace Raven.NewClient.Client.Counters
{
    /// <summary>
    /// The set of conventions used by the <see cref="CountersConvention"/> which allow the users to customize
    /// the way the Raven client API behaves
    /// </summary>
    public class CountersConvention: ConventionBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CountersConvention"/> class.
        /// </summary>
        public CountersConvention()
        {
            FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
            AllowMultipleAsyncOperations = true;
            ShouldCacheRequest = url => true;
        }
    }
}
