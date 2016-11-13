using Raven.Abstractions.Indexing;
using Raven.NewClient.Client.Indexing;

namespace Raven.Abstractions.Replication
{
    public class SideBySideReplicationInfo
    {
        public IndexDefinition Index { get; set; }

        public IndexDefinition SideBySideIndex { get; set; }

        public IndexReplaceDocument IndexReplaceDocument { get; set; }

        public string OriginDatabaseId { get; set; }
    }
}
