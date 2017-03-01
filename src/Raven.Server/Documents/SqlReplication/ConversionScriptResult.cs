using System.Collections.Generic;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationScriptResult
    {
        public readonly Dictionary<string, List<ItemToReplicate>> Data = new Dictionary<string, List<ItemToReplicate>>();
        public readonly List<string> Keys = new List<string>();
    }

    public class ItemToReplicate
    {
        public string DocumentKey { get; set; }

        public List<SqlReplicationColumn> Columns { get; set; }
    }
}