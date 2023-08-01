using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class DeleteCompareExchangeCommandData : ICommandData
    {
        public readonly long Index;

        public DeleteCompareExchangeCommandData(string key, long index)
        {
            Id = key;
            Index = index;
        }

        public string Id { get; }
        public string Name => null;
        public string ChangeVector => null;
        public CommandType Type => CommandType.CompareExchangeDELETE;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Index)] = Index,
                [nameof(Type)] = nameof(CommandType.CompareExchangeDELETE),
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
