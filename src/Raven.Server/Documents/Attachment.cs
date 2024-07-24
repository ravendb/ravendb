using System;
using System.IO;
using Raven.Client.Documents.Attachments;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public sealed class Attachment
    {
        public long StorageId;
        public LazyStringValue Key;
        public long Etag;
        public string ChangeVector;
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Base64Hash;
        public Stream Stream;
        public short TransactionMarker;
        public long Size;
        public AttachmentFlags Flags;
        public DateTime? RetiredAt;
    }
}
