using Raven.Server.Json;

namespace Raven.Server.Documents
{
    public class DocumentTombstone
    {
        public LazyStringValue Key;

        public long DeletedEtag;

        public LazyStringValue Collection;

        public long Etag;

        public long StorageId;
    }
}