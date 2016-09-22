using Raven.Client.Replication.Messages;

namespace Raven.Server.Documents.Indexes
{
    public struct IndexMetadata
    {
        public int IndexId;
        public ChangeVectorEntry[] ChangeVector;
        public long Etag;
    }
}