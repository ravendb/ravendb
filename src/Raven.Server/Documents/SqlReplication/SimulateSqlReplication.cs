namespace Raven.Server.Documents.SqlReplication
{
    public class SimulateSqlReplication
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
        public SqlReplicationConfiguration Configuration;
    }
}