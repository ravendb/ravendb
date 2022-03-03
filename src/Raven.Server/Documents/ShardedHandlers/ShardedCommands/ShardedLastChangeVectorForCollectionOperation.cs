using System;
using System.Linq;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public readonly struct ShardedLastChangeVectorForCollectionOperation : IShardedOperation<LastChangeVectorForCollectionResult>
    {
        private readonly string _collection;

        public ShardedLastChangeVectorForCollectionOperation(string collection)
        {
            _collection = collection;
        }

        public LastChangeVectorForCollectionResult Combine(Memory<LastChangeVectorForCollectionResult> results)
        {
            return new LastChangeVectorForCollectionResult
            {
                Collection = _collection,
                LastChangeVector = ChangeVectorUtils.MergeVectors(results.ToArray().Select(x => x.LastChangeVector).ToArray())
            };
        }

        public RavenCommand<LastChangeVectorForCollectionResult> CreateCommandForShard(int shard) => new LastChangeVectorForCollectionCommand(_collection);
    }

    public class LastChangeVectorForCollectionCommand : RavenCommand<LastChangeVectorForCollectionResult>
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

    public class LastChangeVectorForCollectionResult
    {
        public string Collection { get; set; }
        public string LastChangeVector { get; set; }
    }
}
