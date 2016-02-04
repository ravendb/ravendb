using Raven.Server.Json;

namespace Raven.Server.Documents
{
    public class Document
    {
        public long Etag;
        public string Key;
        public long StorageId;
        public BlittableJsonReaderObject Data;
    }
}