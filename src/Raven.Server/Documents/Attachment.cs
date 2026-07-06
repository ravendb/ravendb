using System.IO;
using Raven.Client.Documents.Operations.Attachments;
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
        public RemoteAttachmentParameters RemoteParameters;

        // Parent revision's version-only CV; populated only on revision-attachment rows (AttachmentsTable.RevisionVersion field 11), null on doc attachments and legacy rows.
        public string RevisionVersion;
    }
}
