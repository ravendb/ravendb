using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct ShardedLastChangeVectorForCollectionOperation : IShardedOperation<LastChangeVectorForCollectionResult, LastChangeVectorForCollectionCombinedResult>
{
    private readonly ShardedSubscriptionsHandler _shardedSubscriptionsHandler;
    private readonly string _collection;
    private readonly string _database;

    public ShardedLastChangeVectorForCollectionOperation(ShardedSubscriptionsHandler shardedSubscriptionsHandler, string collection, string database)
    {
        _shardedSubscriptionsHandler = shardedSubscriptionsHandler;
        _collection = collection ?? throw new ArgumentNullException(nameof(collection));
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public HttpRequest HttpRequest => _shardedSubscriptionsHandler.HttpContext.Request;

    public LastChangeVectorForCollectionCombinedResult Combine(Memory<LastChangeVectorForCollectionResult> results)
    {
        var dic = new Dictionary<string, string>();
        var array = results.Span;
        for (var i = 0; i < array.Length; i++)
        {
            dic.Add($"{_database}${i}", array[i].LastChangeVector);
        }

        return new LastChangeVectorForCollectionCombinedResult
        {
            Collection = _collection,
            LastChangeVectors = dic
        };
    }

    public RavenCommand<LastChangeVectorForCollectionResult> CreateCommandForShard(int shard) => new LastChangeVectorForCollectionCommand(_collection);

    private class LastChangeVectorForCollectionCommand : RavenCommand<LastChangeVectorForCollectionResult>
    {
        private readonly string _collection;

        public LastChangeVectorForCollectionCommand(string collection)
        {
            _collection = collection;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/collections/last-change-vector?name={Uri.EscapeDataString(_collection)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationServer.LastChangeVectorForCollectionResult(response);
        }

        public override bool IsReadRequest => true;
    }
}

public class LastChangeVectorForCollectionResult
{
    public string Collection { get; set; }
    public string LastChangeVector { get; set; }
}

public class LastChangeVectorForCollectionCombinedResult
{
    public string Collection { get; set; }
    public Dictionary<string, string> LastChangeVectors { get; set; }
}
