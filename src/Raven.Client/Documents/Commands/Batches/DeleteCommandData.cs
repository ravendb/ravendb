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
            :this(id, changeVector, changeVector)
        {
            
        }
        public DeleteCommandData(string id, string changeVector, string originalChangeVector)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            ChangeVector = changeVector;
            OriginalChangeVector = originalChangeVector;
        }

        public string OriginalChangeVector { get; }
        public string Id { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }
        public CommandType Type { get; } = CommandType.DELETE;
        public BlittableJsonReaderObject Document { get; set; }

        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(Document)] = Document
            };
            if (OriginalChangeVector != null)
            {
                json[nameof(OriginalChangeVector)] = OriginalChangeVector;
            }
            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeDeleteInvoke(new BeforeDeleteEventArgs(session, Id, null));
        }
    }
}
