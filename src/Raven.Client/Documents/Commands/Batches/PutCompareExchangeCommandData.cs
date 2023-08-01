using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class PutCompareExchangeCommandData : ICommandData
    {
        public readonly long Index;
        public BlittableJsonReaderObject Document { get; }

        public PutCompareExchangeCommandData(string key, BlittableJsonReaderObject value, long index)
        {
            Id = key;
            Document = value;
            Index = index;
        }

        public string Id { get; }
        public string Name => null;
        public string ChangeVector => null;
        public CommandType Type => CommandType.CompareExchangePUT;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Document)] = Document,
                [nameof(Index)] = Index,
                [nameof(Type)] = nameof(CommandType.CompareExchangePUT)
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
