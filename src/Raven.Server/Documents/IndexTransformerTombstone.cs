using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    //tombstone for deleted indexes and transformers
    public struct IndexTransformerTombstone
    {
        //we have to use name here because index/transformer Id might get reused
        public LazyStringValue Name;
        public LazyStringValue LoweredName;

        public MetadataStorageType Type;

        public long Etag;

        public long DeletedEtag;

        public ChangeVectorEntry[] ChangeVector;
    }
}
