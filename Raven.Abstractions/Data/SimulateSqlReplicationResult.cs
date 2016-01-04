using System;
using System.Collections.Generic;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Abstractions.Data
{
    /// <summary>
    /// The result of a query
    /// </summary>
    public class SimulateSqlReplicationResult
    {
        /// <summary>
        /// Document Id to simulate replication on
        /// </summary>
        public string DocumentId;
        /// <summary>
        /// Perform Rolled Back Transaction
        /// </summary>
        public bool PerformRolledBackTransaction;
        /// <summary>
        /// Sql Replication Script
        /// </summary>
        public string SqlReplication;
    }
}