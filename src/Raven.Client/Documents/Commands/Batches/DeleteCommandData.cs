using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class DeleteCommandData : ICommandData
    {
        public DeleteCommandData(string id, string changeVector)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            ChangeVector = changeVector;
        }

        public string Id { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }
        public CommandType Type { get; } = CommandType.DELETE;
        public BlittableJsonReaderObject Document { get; set; }

        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(Document)] = Document
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeDeleteInvoke(new BeforeDeleteEventArgs(session, Id, null));
        }
    }
}
