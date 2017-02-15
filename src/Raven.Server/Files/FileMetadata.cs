using System;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Files
{
    public class FileMetadata : IDisposable
    {
        public long Etag;
        public LazyStringValue Name;
        public LazyStringValue LoweredKey;
        public long StorageId;
        public BlittableJsonReaderObject Metadata;

        public DateTime LastModifed;

        public Slice StreamIdentifier;
        public IDisposable StreamIdentifierDispose;

        public void Dispose()
        {
            Metadata?.Dispose();
            Name?.Dispose();
            LoweredKey?.Dispose();
            StreamIdentifierDispose?.Dispose();
        }
    }
}