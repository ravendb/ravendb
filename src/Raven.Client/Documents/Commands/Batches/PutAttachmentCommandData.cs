using System;
using System.Diagnostics;
using System.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class PutAttachmentCommandData : ICommandData
    {
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector)
            : this(documentId, name, stream, contentType, changeVector, retireAt: null, stream.Length, flags: AttachmentFlags.None, hash: null, fromEtl: false)
        {
        }

        internal PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, DateTime? retireAt, long size, AttachmentFlags flags, string hash, bool fromEtl)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            Stream = stream;
            ContentType = contentType;
            ChangeVector = changeVector;
            FromEtl = fromEtl;
            RetiredAt = retireAt;
            Size = size;
            Flags = flags;
            Hash = hash;
            if (Flags.Contain(AttachmentFlags.Retired))
            {
                Debug.Assert(Stream == null, "Stream == null");
            }
            else
            {
                PutAttachmentCommandHelper.ValidateStream(stream);
            }
        }

        public string Id { get; }
        public string Name { get; }
        public Stream Stream { get; }
        public string ChangeVector { get; }
        public string ContentType { get; }
        public CommandType Type { get; } = CommandType.AttachmentPUT;
        public bool FromEtl { get; }
        public DateTime? RetiredAt { get; }
        public long Size { get; }
        public AttachmentFlags Flags { get; }
        public string Hash { get; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ContentType)] = ContentType,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(FromEtl)] = FromEtl,
                [nameof(RetiredAt)] = RetiredAt,
                [nameof(Size)] = Size,
                [nameof(Flags)] = Flags.ToString(),
                [nameof(Hash)] = Hash

            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
