using System;
using System.IO;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public class Attachment
    {
        public long StorageId;
        public LazyStringValue Key;
        public long Etag;
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Base64Hash;
        public Stream Stream;
        public short TransactionMarker;
        public long Size;
    }
}