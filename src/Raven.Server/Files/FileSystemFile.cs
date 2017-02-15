using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Server.Files
{
    public class FileSystemFile
    {
        public long Etag;
        public LazyStringValue Name;
        public LazyStringValue LoweredKey;
        public long StorageId;
        public BlittableJsonReaderObject Metadata;

        public DateTime LastModifed;

        public Stream Stream;
    }
}