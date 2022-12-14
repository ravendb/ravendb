using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Executors;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    internal readonly struct ShardedGetCollectionFieldsOperation : IShardedReadOperation<BlittableJsonReaderObject, Dictionary<LazyStringValue, FieldType>>
    {
        private readonly JsonOperationContext _context;
        private readonly HttpContext _httpContext;
        private readonly string _collection;
        private readonly string _prefix;

        public ShardedGetCollectionFieldsOperation(JsonOperationContext context, HttpContext httpContext, string collection, string prefix, string etag)
        {
            _context = context;
            _httpContext = httpContext;
            _collection = collection;
            _prefix = prefix;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public string ExpectedEtag { get; }

        public Dictionary<LazyStringValue, FieldType> CombineResults(Dictionary<int, ShardExecutionResult<BlittableJsonReaderObject>> results)
        {
            var combined = new Dictionary<LazyStringValue, FieldType>(LazyStringValueComparer.Instance);

            foreach (var collectionFields in results.Values)
            {
                if (collectionFields.Result == null)
                    continue;

                var propDetails = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < collectionFields.Result.Count; i++)
                {
                    collectionFields.Result.GetPropertyByIndex(i, ref propDetails);
                    if (Enum.TryParse(propDetails.Value.ToString(), out FieldType type))
                        combined.TryAdd(propDetails.Name.Clone(_context), type);
                }
            }

            return combined;
        }

        public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber) => new GetCollectionFieldsCommand(_collection, _prefix);
    }
}
