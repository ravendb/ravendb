using System.Collections.Generic;

namespace Raven.Bundles.ReplicateToSql.Data
{
    public class ReplicateToSqlDestination
    {
        public string Id { get; set; }
        public string ConnectionStringName { get; set; }
        public string TableName { get; set; }
        public string PrimaryKeyColumnName { get; set; }
        public IDictionary<string, string> ColumnsMapping { get; set; }

        public ReplicateToSqlDestination()
        {
            ColumnsMapping = new Dictionary<string, string>();
        }
    }
}