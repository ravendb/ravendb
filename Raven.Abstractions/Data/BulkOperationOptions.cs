using System;
using System.Threading;


namespace Raven.Abstractions.Data
{
    /// <summary>
    /// Holds diffrents setting options for base operations.
    /// </summary>
    public class BulkOperationOptions
    {
        /// <summary>
        /// indicates whether operations are allowed on stale indexes.
        /// </summary>
         public bool AllowStale { get; set; }

         public TimeSpan? StaleTimeout { get; set; }

        /// <summary>
        /// limits the amount of base operation per second allowed.
        /// </summary>
         public int? MaxOpsPerSec { get; set; }

    }
}
