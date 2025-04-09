using System;
using Raven.Client.Documents.Commands.Batches;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Holds different setting options for base operations.
    /// </summary>
    public sealed class QueryOperationOptions
    {
        private int? _maxOpsPerSecond;

        /// <summary>
        /// Indicates whether operations are allowed on stale indexes.
        /// </summary>
        public bool AllowStale { get; set; }

        /// <summary>
        /// Ignore the maximum number of statements a script can execute as defined in the server configuration.
        /// </summary>
        public bool IgnoreMaxStepsForScript { get; set; }

        /// <summary>
        /// If AllowStale is set to false and index is stale, then this is the maximum timeout to wait for index to become non-stale. If timeout is exceeded then exception is thrown.
        /// <para>Value:</para>
        /// <para><c>null</c> by default - throw immediately if index is stale</para>
        /// </summary>
        /// <value>null by default - throw immediately if index is stale</value>
        public TimeSpan? StaleTimeout { get; set; }

        /// <summary>
        /// Limits the amount of base operation per second allowed.
        /// </summary>
        public int? MaxOpsPerSecond
        {
            get => _maxOpsPerSecond;

            set
            {
                if (value.HasValue && value.Value <= 0)
                    throw new InvalidOperationException("MaxOpsPerSecond must be greater than 0");

                _maxOpsPerSecond = value;
            }
        }

        /// <summary>
        /// Determines whether operation details about each document should be returned by server.
        /// </summary>
        public bool RetrieveDetails { get; set; }


        /// <summary>
        /// Encapsulates advanced options for waiting on indexes during bulk or patch operations.
        /// When WaitForIndexes is true, the operation will wait until the affected indexes become non-stale.
        /// WaitForIndexesTimeout specifies the maximum duration to wait, and if this period is exceeded while
        /// ThrowOnTimeoutInWaitForIndexes is true, an exception will be thrown.
        /// If WaitForSpecificIndexes contains one or more index names, the operation will restrict the wait
        /// to those indexes only; otherwise, all relevant indexes are considered.
        /// </summary>
        public IndexBatchOptions IndexOptions { get; set; }
    }
}
