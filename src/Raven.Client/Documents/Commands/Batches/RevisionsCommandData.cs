using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class ForceRevisionCommandData : ICommandData
    {
        public ForceRevisionCommandData(string id)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
        }

        public CommandType Type { get; } = CommandType.ForceRevisionCreation;
        public string Id { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }
    
        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Type)] = Type.ToString()
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
