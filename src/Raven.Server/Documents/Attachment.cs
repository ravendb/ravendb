using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class Attachment
    {
        public long Etag;
        public LazyStringValue LoweredDocumentId;
        public LazyStringValue LoweredName;
        public long StorageId;

        public DateTime LastModified;

        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Stream Stream;
    }
}