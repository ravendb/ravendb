using Raven.Client.Replication.Messages;

namespace Raven.Server.Documents.Indexes
{
    public struct IndexTransformerMetadata
    {
        public int Id;
        public ChangeVectorEntry[] ChangeVector;
        public long Etag;
    }
}