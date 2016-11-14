using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexing;

namespace Raven.NewClient.Abstractions.Replication
{
    public class SideBySideReplicationInfo
    {
        public IndexDefinition Index { get; set; }

        public IndexDefinition SideBySideIndex { get; set; }

        public IndexReplaceDocument IndexReplaceDocument { get; set; }

        public string OriginDatabaseId { get; set; }
    }
}
