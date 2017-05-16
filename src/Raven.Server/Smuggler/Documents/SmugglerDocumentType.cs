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
        Attachment = 2,
    }

    public class DocumentItem
    {
        public const string Key = "@document-type";

        public Document Document;
        public List<AttachmentStream> Attachments;

        public struct AttachmentStream : IDisposable
        {
            public Slice Base64Hash;
            public ByteStringContext<ByteStringMemoryCache>.ExternalScope Base64HashDispose;

            public Stream File;
            public AttachmentsStorage.ReleaseTempFile FileDispose;

            public BlittableJsonReaderObject Data;

            public void Dispose()
            {
                Base64HashDispose.Dispose();
                FileDispose.Dispose();
                Data.Dispose();
            }
        }
    }
}