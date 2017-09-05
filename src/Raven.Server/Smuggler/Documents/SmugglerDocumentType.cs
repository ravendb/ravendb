using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Smuggler.Documents
{
    public enum DocumentType : byte
    {
        Document = 1,
        Attachment = 2
    }

    public class DocumentItem
    {
        public const string Key = "@document-type";

        public Document Document;
        public List<AttachmentStream> Attachments;
        public DocumentTombstone Tombstone;

        public struct AttachmentStream : IDisposable
        {
            public Slice Base64Hash;
            public ByteStringContext.ExternalScope Base64HashDispose;

            public Slice Tag;
            public ByteStringContext.ExternalScope TagDispose;

            public Stream Stream;

            public BlittableJsonReaderObject Data;

            public void Dispose()
            {
                Base64HashDispose.Dispose();
                TagDispose.Dispose();
                Stream.Dispose();
                Data.Dispose();
            }
        }
    }
}