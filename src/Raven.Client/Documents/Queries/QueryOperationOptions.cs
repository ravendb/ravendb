using System;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Holds different setting options for base operations.
    /// </summary>
    public class QueryOperationOptions
    {
        private int? _maxOpsPerSecond;

        /// <summary>
        /// Indicates whether operations are allowed on stale indexes.
        /// </summary>
        public bool AllowStale { get; set; }

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
            get
            {
                return _maxOpsPerSecond;
            }

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
    }
}
