using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.OLAP.Test
{
    public class OlapEtlTestScriptResult : TestEtlScriptResult
    {
        public List<PartitionItems> ItemsByPartition { get; set; }

        public class PartitionItems
        {
            public string Key { get; set; }

            public List<PartitionColumn> Columns { get; set; } = new List<PartitionColumn>();
        }

        public class PartitionColumn
        {
            public string Name { get; set; }

            public string Type { get; set; }

            public IList Values { get; set; }
        }
    }
}
