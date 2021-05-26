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
        public DeleteCommandData(string id, string changeVector, string oldChangeVector)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            ChangeVector = changeVector;
            OldChangeVector = oldChangeVector;
        }

        public string OldChangeVector { get; }
        public string Id { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }
        public CommandType Type { get; } = CommandType.DELETE;

        public virtual DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
            };
            if (OldChangeVector != null)
            {
                json[nameof(OldChangeVector)] = OldChangeVector;
            }
            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeDeleteInvoke(new BeforeDeleteEventArgs(session, Id, null));
        }
    }
}
