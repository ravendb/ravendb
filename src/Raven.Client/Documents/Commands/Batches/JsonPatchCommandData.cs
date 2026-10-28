using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class JsonPatchCommandData : ICommandData
    {
        public string Id { get; }
        public JsonPatchDocument JsonPatch { get; }
        public string Name { get; } = null;
        public string ChangeVector { get; }

        private bool ReturnDocument;
        public CommandType Type { get; } = CommandType.JsonPatch;

        public JsonPatchCommandData(string id, JsonPatchDocument patch)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            JsonPatch = patch;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = null,
                [nameof(JsonPatch)] = new DynamicJsonValue
                {
                    ["Operations"] = new DynamicJsonArray(JsonPatch.ToBlittable(conventions.Serialization, context))
                },
                [nameof(ReturnDocument)] = ReturnDocument,
                [nameof(Type)] = Type.ToString()
            };

            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            ReturnDocument = session.IsLoaded(Id);
        }
    }
}
