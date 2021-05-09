using System.Net.Http;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class JsonPatchOperation : IOperation
    {
        public string Id;
        public JsonPatchDocument JsonPatchDocument;

        public JsonPatchOperation(string id, JsonPatchDocument jsonPatchDocument)
        {
            Id = id;
            JsonPatchDocument = jsonPatchDocument;
        }

        public RavenCommand GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new JsonPatchCommand(Id, JsonPatchDocument);
        }

        public class JsonPatchCommand : RavenCommand
        {
            private readonly string _id;
            private readonly JsonPatchDocument _jsonPatchDocument;
            public override bool IsReadRequest => false;

            public JsonPatchCommand(string id, JsonPatchDocument jsonPatchDocument)
            {
                _id = id;
                _jsonPatchDocument = jsonPatchDocument;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/json-patch?id={_id}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName("Operations");

                            writer.WriteStartArray();

                            var serializer = DocumentConventions.Default.Serialization.CreateSerializer(new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.None });
                            foreach (var operation in _jsonPatchDocument.Operations)
                            {
                                writer.WriteValue(BlittableJsonToken.EmbeddedBlittable, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(operation, ctx, serializer));
                            }

                            writer.WriteEndArray();
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }
        }
    }
}
