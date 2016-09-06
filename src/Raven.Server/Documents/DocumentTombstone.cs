using Raven.Client.Replication.Messages;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class DocumentTombstone
    {
        public LazyStringValue Key;

        public LazyStringValue LoweredKey;

        public long DeletedEtag;

        public long Etag;

        public long StorageId;

        public LazyStringValue Collection;

        public ChangeVectorEntry[] ChangeVector;
    }
}