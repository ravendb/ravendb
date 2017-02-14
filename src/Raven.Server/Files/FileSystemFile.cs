using System;
using Sparrow.Json;
using Voron.Data;

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

        public VoronStream Stream;
    }
}