using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class MoveAttachmentCommandData : ICommandData
    {
        public MoveAttachmentCommandData(string documentId, string name, string destinationDocumentId, string destinationName, string changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrWhiteSpace(destinationDocumentId))
                throw new ArgumentNullException(nameof(destinationDocumentId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentNullException(nameof(destinationName));

            Id = documentId;
            Name = name;
            DestinationId = destinationDocumentId;
            DestinationName = destinationName;
            ChangeVector = changeVector;
        }

        public string Id { get; }
        public string Name { get; }
        public string DestinationId { get; }
        public string DestinationName { get; }
        public string ChangeVector { get; }
        public CommandType Type { get; } = CommandType.AttachmentMOVE;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(DestinationId)] = DestinationId,
                [nameof(DestinationName)] = DestinationName,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString()
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
