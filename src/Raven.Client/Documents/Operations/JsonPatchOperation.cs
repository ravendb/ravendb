using System;
using System.Collections.Generic;
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
    /// <summary>
    ///     A class for executing JsonPatch operations on documents in the database
    /// </summary>
    public sealed class JsonPatchOperation : IOperation<JsonPatchResult>
    {
        /// <summary>
        /// The id of the document on which to execute the patch operations
        /// </summary>
        public string Id;

        /// <summary>
        /// A collection of JsonPatch operations to execute on the document. <br/>
        /// <remarks>The operations are executed sequentially by the order of their addition to the collection</remarks>
        /// </summary>
        public JsonPatchDocument JsonPatchDocument;

        /// <summary>
        ///     Executes a collection of JsonPatch operations on a specific document in the database.<br/>
        /// <remarks>
        ///     See <see href="https://ravendb.net/docs/article-page/latest/csharp/client-api/operations/patching/json-patch-syntax">documentation</see>
        /// </remarks>
        /// </summary>
        /// <param name="id">The id of the document on which to execute the <paramref name="jsonPatchDocument"/> operations</param>
        /// <param name="jsonPatchDocument">
        ///     A collection of JsonPatch operations to execute on the document. <br/>
        /// <remarks>The operations are executed sequentially by the order of their addition to the collection</remarks>
        /// </param>
        public JsonPatchOperation(string id, JsonPatchDocument jsonPatchDocument)
        {
            Id = id;
            JsonPatchDocument = jsonPatchDocument;
        }

        public RavenCommand<JsonPatchResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new JsonPatchCommand(conventions, Id, JsonPatchDocument);
        }

        internal sealed class JsonPatchCommand : RavenCommand<JsonPatchResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly string _id;
            private readonly JsonPatchDocument _jsonPatchDocument;
            public override bool IsReadRequest => false;

            public JsonPatchCommand(DocumentConventions conventions, string id, JsonPatchDocument jsonPatchDocument)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _id = id;
                _jsonPatchDocument = jsonPatchDocument;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/json-patch?id={UrlEncode(_id)}";
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            var serializer = _conventions.Serialization.CreateSerializer(new CreateSerializerOptions { TypeNameHandling = TypeNameHandling.None });
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(JsonOperation.Operations));
                            writer.WriteStartArray();
                            var isFirst = true;
                            foreach (var operation in _jsonPatchDocument.Operations)
                            {
                                if (isFirst)
                                    isFirst = false;
                                else
                                    writer.WriteComma();
                                
                                ctx.Write(writer, _conventions.Serialization.DefaultConverter.ToBlittable(
                                    operation
                                    , ctx, serializer));
                            }
                    
                            writer.WriteEndArray();
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };
                
                return request;
            }
            
            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;
                if (fromCache) 
                {
                    // we have to clone the response here because  otherwise the cached item might be freed while
                    // we are still looking at this result, so we clone it to the side
                    response = response.Clone(context);
                }
                Result = JsonDeserializationClient.JsonPatchResult(response);
            }
        }
    }

    internal sealed class JsonOperation
    {
        public List<Microsoft.AspNetCore.JsonPatch.Operations.Operation> Operations { get; set; }
    }
}
