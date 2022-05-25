using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    internal readonly struct ShardedGetCollectionFieldsOperation : IShardedOperation<BlittableJsonReaderObject, Dictionary<LazyStringValue, FieldType>>
    {
        private readonly JsonOperationContext _context;
        private readonly HttpContext _httpContext;
        private readonly string _collection;
        private readonly string _prefix;

        public ShardedGetCollectionFieldsOperation(JsonOperationContext context, HttpContext httpContext, string collection, string prefix)
        {
            _context = context;
            _httpContext = httpContext;
            _collection = collection;
            _prefix = prefix;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public Dictionary<LazyStringValue, FieldType> Combine(Memory<BlittableJsonReaderObject> results)
        {
            var span = results.Span;
            var combined = new Dictionary<LazyStringValue, FieldType>(LazyStringValueComparer.Instance);
            
            foreach (var collectionFields in span)
            {
                if (collectionFields == null)
                    continue;

                var propDetails = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < collectionFields.Count; i++)
                {
                    collectionFields.GetPropertyByIndex(i, ref propDetails);
                    if(Enum.TryParse(propDetails.Value.ToString(), out FieldType type))
                        combined.TryAdd(propDetails.Name.Clone(_context), type);
                }
            }

            return combined;
        }

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => new GetCollectionFieldsCommand(_collection, _prefix);
    }

    public class GetCollectionFieldsCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly string _collection;
        private readonly string _prefix;

        public GetCollectionFieldsCommand(string collection, string prefix)
        {
            _collection = collection;
            _prefix = prefix;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/studio/collections/fields");

            if (string.IsNullOrEmpty(_collection) == false && string.IsNullOrEmpty(_prefix) == false)
            {
                pathBuilder.Append($"?collection={Uri.EscapeDataString(_collection)}");
                pathBuilder.Append($"&prefix={Uri.EscapeDataString(_prefix)}");
            }
            else if (string.IsNullOrEmpty(_collection) == false)
                pathBuilder.Append($"?collection={Uri.EscapeDataString(_collection)}");
            else if (string.IsNullOrEmpty(_prefix) == false)
                pathBuilder.Append($"?prefix={Uri.EscapeDataString(_prefix)}");

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = response.Clone(context);
        }

        public override bool IsReadRequest => true;
    }
}
