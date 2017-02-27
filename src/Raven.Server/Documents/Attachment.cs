using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class Attachment
    {
        public long StorageId;
        public LazyStringValue LoweredKey;
        public long Etag;
        public DateTime LastModified;
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Stream Stream;
    }
}