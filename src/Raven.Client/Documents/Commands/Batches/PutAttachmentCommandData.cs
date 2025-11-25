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
            : this(documentId, name, stream, contentType, changeVector, size: null, remoteAttachmentParameters: null, hash: null, fromEtl: false)
        {
        }

        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, RemoteAttachmentParameters remoteAttachmentParameters) 
            : this(documentId, name, stream, contentType, changeVector, size: null, remoteAttachmentParameters: remoteAttachmentParameters, hash: null, fromEtl: false)
        {
        }

        internal PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, long? size, RemoteAttachmentParameters remoteAttachmentParameters, string hash, bool fromEtl)
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
            RemoteParameters = remoteAttachmentParameters;
            Hash = hash;

            // when this is called from ETL we know the size in advance
            SizeInBytes = size;

            PutAttachmentCommandHelper.TryValidateStream(stream, RemoteParameters);
        }

        public string Id { get; }
        public string Name { get; }
        public Stream Stream { get; }
        public string ChangeVector { get; }
        public string ContentType { get; }
        public CommandType Type { get; } = CommandType.AttachmentPUT;
        public RemoteAttachmentParameters RemoteParameters { get; }
        internal string Hash { get; }
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
                [nameof(RemoteParameters)] = RemoteParameters?.ToJson(),
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
