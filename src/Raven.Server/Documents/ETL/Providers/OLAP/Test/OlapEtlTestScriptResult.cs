using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.OLAP.Test
{
    public sealed class OlapEtlTestScriptResult : TestEtlScriptResult
    {
        public List<PartitionItems> ItemsByPartition { get; set; }

        public sealed class PartitionItems
        {
            public string Key { get; set; }

            public List<PartitionColumn> Columns { get; set; } = new List<PartitionColumn>();
        }

        public sealed class PartitionColumn
        {
            public string Name { get; set; }

            public string Type { get; set; }

            public IList Values { get; set; }
        }
    }
}
