﻿using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class JsonPatchOperation : IOperation<JsonPatchResult>
    {
        public string Id;
        public JsonPatchDocument JsonPatchDocument;

        public JsonPatchOperation(string id, JsonPatchDocument jsonPatchDocument)
        {
            Id = id;
            JsonPatchDocument = jsonPatchDocument;
        }

        public RavenCommand<JsonPatchResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new JsonPatchCommand(conventions, Id, JsonPatchDocument);
        }

        internal class JsonPatchCommand : RavenCommand<JsonPatchResult>
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
                
                var operationDjv = new DynamicJsonValue
                {
                    [nameof(JsonOperation.Operations)] =  TypeConverter.ToBlittableSupportedType(_jsonPatchDocument.Operations, _conventions, ctx)
                };

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            ctx.Write(writer, ctx.ReadObject(operationDjv, _id));
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

    internal class JsonOperation
    {
        public List<Microsoft.AspNetCore.JsonPatch.Operations.Operation> Operations { get; set; }
    }
}
