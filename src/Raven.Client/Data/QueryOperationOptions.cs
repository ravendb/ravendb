using System;

namespace Raven.Client.Data
{
    /// <summary>
    /// Holds different setting options for base operations.
    /// </summary>
    public class QueryOperationOptions
    {
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
        public int? MaxOpsPerSecond { get; set; }

        /// <summary>
        /// Determines whether operation details about each document should be returned by server.
        /// </summary>
        public bool RetrieveDetails { get; set; }
    }
}
