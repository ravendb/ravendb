using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class DocumentTombstone
    {
        public LazyStringValue Key;

        public long DeletedEtag;

        public long Etag;

        public long StorageId;
    }
}