using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetTombstones : AbstractReplicationHandlerProcessorForGetTombstones<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        private ShardedPagingContinuation _continuationToken;

        public ShardedReplicationHandlerProcessorForGetTombstones([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<GetTombstonesPreviewResult> GetTombstonesAsync(TransactionOperationContext context, int start, int pageSize)
        {
            _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, start, pageSize);

            var op = new ShardedGetAllTombstonesOperation(RequestHandler, _continuationToken);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            result.ContinuationToken = _continuationToken.ToBase64(context);

            return result;
        }

        internal readonly struct ShardedGetAllTombstonesOperation : IShardedOperation<GetTombstonesPreviewResult>
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly ShardedPagingContinuation _token;

            public ShardedGetAllTombstonesOperation(ShardedDatabaseRequestHandler handler, ShardedPagingContinuation continuationToken)
            {
                _handler = handler;
                _token = continuationToken;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;
            public GetTombstonesPreviewResult Combine(Memory<GetTombstonesPreviewResult> results)
            {
                var final = new GetTombstonesPreviewResult();

                final.Tombstones = _handler.DatabaseContext.Streaming.PagedShardedItem(
                    results,
                    selector: r => r.Tombstones,
                    comparer: TombstonesLastModifiedComparer.Instance,
                    _token).ToList();

                return final;
            }

            public RavenCommand<GetTombstonesPreviewResult> CreateCommandForShard(int shardNumber) => new GetReplicationTombstonesCommand(_token.Pages[shardNumber].Start, _token.PageSize);
        }

        internal class GetReplicationTombstonesCommand : RavenCommand<GetTombstonesPreviewResult>
        {
            private readonly long _start;
            private readonly int? _pageSize;
            [CanBeNull]
            private readonly string _token;

            public override bool IsReadRequest => true;

            public GetReplicationTombstonesCommand(long start = 0, int pageSize = int.MaxValue)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public GetReplicationTombstonesCommand(string continuationToken)
            {
                _token = continuationToken;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var sb = new StringBuilder();
                sb.Append($"{node.Url}/databases/{node.Database}/replication/tombstones");

                if (_token != null)
                    sb.Append($"?{ContinuationToken.ContinuationTokenQueryString}={Uri.EscapeDataString(_token)}");
                else
                {
                    sb.Append($"?start={_start}");

                    if (_pageSize.HasValue && _pageSize != int.MaxValue)
                        sb.Append($"&pageSize={_pageSize}");
                }

                url = sb.ToString();

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var tombstones = new List<Tombstone>();
                if (response.TryGet(nameof(GetTombstonesPreviewResult.Tombstones), out BlittableJsonReaderArray bjra))
                {
                    foreach (BlittableJsonReaderObject bjro in bjra)
                    {
                        var tombstone = Tombstone.FromJson(context, bjro);
                        tombstones.Add(tombstone);
                    }
                }

                response.TryGet(nameof(GetTombstonesPreviewResult.ContinuationToken), out string token);

                Result = new GetTombstonesPreviewResult {Tombstones = tombstones, ContinuationToken = token};
            }
        }

        public class TombstonesLastModifiedComparer : Comparer<ShardStreamItem<Tombstone>>
        {
            public override int Compare(ShardStreamItem<Tombstone> x,
                ShardStreamItem<Tombstone> y)
            {
                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return TombstonesPreviewComparer.Instance.Compare(x.Item, y.Item);
            }

            public static TombstonesLastModifiedComparer Instance = new();
        }

        public class TombstonesPreviewComparer : Comparer<Tombstone>
        {
            public override int Compare(Tombstone x, Tombstone y)
            {
                if (x == null)
                    return -1;
                if (y == null)
                    return -1;

                if (x.LastModified.Ticks == y.LastModified.Ticks)
                    return 0;

                if (x.LastModified.Ticks < y.LastModified.Ticks)
                    return -1;

                return 1;
            }

            public static TombstonesPreviewComparer Instance = new();
        }
    }
}
