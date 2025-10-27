using System;
using System.IO;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class PutAttachmentCommandData : ICommandData
    {
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector)
            : this(documentId, name, stream, contentType, changeVector, size: null, retireAttachmentParameters: null, hash: null, fromEtl: false)
        {
        }

        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, RetireAttachmentParameters retireAttachmentParameters) 
            : this(documentId, name, stream, contentType, changeVector, size: null, retireAttachmentParameters: retireAttachmentParameters, hash: null, fromEtl: false)
        {
        }

        internal PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, long? size, RetireAttachmentParameters retireAttachmentParameters, string hash, bool fromEtl)
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
            RetireParameters = retireAttachmentParameters;
            Hash = hash;

            // when this is called from ETL we know the size in advance
            SizeInBytes = size;

            PutAttachmentCommandHelper.TryValidateStream(stream, RetireParameters);
        }

        public string Id { get; }
        public string Name { get; }
        public Stream Stream { get; }
        public string ChangeVector { get; }
        public string ContentType { get; }
        public CommandType Type { get; } = CommandType.AttachmentPUT;
        public RetireAttachmentParameters RetireParameters { get; }
        public string Hash { get; }
        internal bool FromEtl { get; }
        internal long? SizeInBytes { get; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var djv = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ContentType)] = ContentType,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(FromEtl)] = FromEtl,
                [nameof(RetireParameters)] = RetireParameters?.ToJson(),
                [nameof(Hash)] = Hash
            };

            if (SizeInBytes.HasValue)
            {
                djv[nameof(SizeInBytes)] = SizeInBytes.Value;
            }

            return djv;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
