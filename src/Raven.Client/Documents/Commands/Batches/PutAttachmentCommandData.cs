using System;
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
        [Obsolete("This constructor is deprecated and will be removed in next RavenDB major version, use the overload with retireAt instead")]
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector)
            : this(documentId, name, stream, contentType, changeVector, retireAt: null, size: null, flags: AttachmentFlags.None, hash: null, fromEtl: false)
        {
            SizeInBytes = stream.Length;
        }

        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, DateTime? retireAt) 
            : this(documentId, name, stream, contentType, changeVector, retireAt, size: null, flags: AttachmentFlags.None, hash: null, fromEtl: false)
        {
            SizeInBytes = stream.Length;
        }

        internal PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, DateTime? retireAt, long? size, AttachmentFlags flags, string hash, bool fromEtl)
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
            RetireAt = retireAt;
            Flags = flags;
            Hash = hash;

            if (size != null)
                SizeInBytes = size.Value;

            PutAttachmentCommandHelper.TryValidateStream(flags, stream);
        }

        public string Id { get; }
        public string Name { get; }
        public Stream Stream { get; }
        public string ChangeVector { get; }
        public string ContentType { get; }
        public CommandType Type { get; } = CommandType.AttachmentPUT;
        internal bool FromEtl { get; }
        public DateTime? RetireAt { get; }
        public long SizeInBytes { get; }
        internal AttachmentFlags Flags { get; }
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
                [nameof(RetireAt)] = RetireAt,
                [nameof(SizeInBytes)] = SizeInBytes,
                [nameof(Flags)] = Flags.ToString(),
                [nameof(Hash)] = Hash

            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
