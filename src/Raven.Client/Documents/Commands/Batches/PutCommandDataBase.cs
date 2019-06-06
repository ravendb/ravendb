using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    internal class PutCommandDataWithBlittableJson : PutCommandDataBase<BlittableJsonReaderObject>
    {
        public PutCommandDataWithBlittableJson(string id, string changeVector, BlittableJsonReaderObject document, bool forceRevisionCreation = false)
            : base(id, changeVector, document, forceRevisionCreation)
        {
        }

        public override void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }

    public class PutCommandData : PutCommandDataBase<DynamicJsonValue>
    {
        public PutCommandData(string id, string changeVector, DynamicJsonValue document, bool forceRevisionCreation = false)
            : base(id, changeVector, document, forceRevisionCreation)
        {
        }

        public override void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }

    public abstract class PutCommandDataBase<T> : ICommandData
    {
        protected PutCommandDataBase(string id, string changeVector, T document, bool forceRevisionCreation = false)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            Id = id;
            ChangeVector = changeVector;
            Document = document;
            ForceRevisionCreation = forceRevisionCreation;
        }

        public string Id { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }
        public T Document { get; }
        public CommandType Type { get; } = CommandType.PUT;
        public bool ForceRevisionCreation { get; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Document)] = Document,
                [nameof(Type)] = Type.ToString(),
                [nameof(ForceRevisionCreation)] = ForceRevisionCreation
            };
        }

        public abstract void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session);
    }
}
