using System;
using System.Linq;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class JsonPatchCommandData : ICommandData
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
            var serializer = DocumentConventions.Default.Serialization.CreateSerializer(new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.None });
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = null,
                [nameof(JsonPatch)] = new DynamicJsonValue
                {
                    ["Operations"] = 
                        new DynamicJsonArray(
                            JsonPatch.Operations.Select(o=> DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(o, context, serializer)))
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
