using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class RenameAttachmentCommandData : ICommandData
    {
        public RenameAttachmentCommandData(string documentId, string name, string newName, string changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrWhiteSpace(newName))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            NewName = newName;
            ChangeVector = changeVector;
        }

        public string Id { get; }
        public string Name { get; }
        public string NewName { get; }
        public string ChangeVector { get; }
        public CommandType Type { get; } = CommandType.AttachmentRENAME;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(NewName)] = NewName,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString()
            };
        }
    }
}
