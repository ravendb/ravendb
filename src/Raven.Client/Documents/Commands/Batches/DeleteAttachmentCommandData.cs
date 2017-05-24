using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class DeleteAttachmentCommandData : ICommandData
    {
        public DeleteAttachmentCommandData(string documentId, string name, long? etag)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            Etag = etag;
        }

        public string Id { get; }
        public string Name { get; }
        public long? Etag { get; }
        public CommandType Type { get; } = CommandType.AttachmentDELETE;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.ToString(),
            };
        }
    }
}