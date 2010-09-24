using System.Collections.Generic;

namespace Raven.Bundles.IndexReplication.Data
{
    public class IndexReplicationDestination
    {
        public string Id { get; set; }
        public string ConnectionStringName { get; set; }
        public string TableName { get; set; }
        public string PrimaryKeyColumnName { get; set; }
        public IDictionary<string, string> ColumnsMapping { get; set; }

        public IndexReplicationDestination()
        {
            ColumnsMapping = new Dictionary<string, string>();
        }
    }
}