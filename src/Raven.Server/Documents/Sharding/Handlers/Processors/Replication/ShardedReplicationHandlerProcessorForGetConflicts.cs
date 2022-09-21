using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetConflicts : AbstractReplicationHandlerProcessorForGetConflicts<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        private ShardedPagingContinuation _continuationToken;

        public ShardedReplicationHandlerProcessorForGetConflicts([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<GetConflictsPreviewResult> GetConflictsPreviewAsync(TransactionOperationContext context, long start, int pageSize)
        {
            _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, (int)start, pageSize);

            var op = new ShardedGetReplicationConflictsOperation(RequestHandler, _continuationToken);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            result.ContinuationToken = _continuationToken.ToBase64(context);

            return result;
        }

        protected override async Task GetConflictsForDocumentAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetConflictsCommand(id: documentId);
            var proxyCommand = new ProxyCommand<GetConflictsResult>(cmd, RequestHandler.HttpContext.Response);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber);
        }

        internal readonly struct ShardedGetReplicationConflictsOperation : IShardedOperation<GetConflictsPreviewResult>
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly ShardedPagingContinuation _token;

            public ShardedGetReplicationConflictsOperation(ShardedDatabaseRequestHandler handler, ShardedPagingContinuation continuationToken)
            {
                _handler = handler;
                _token = continuationToken;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;

            public GetConflictsPreviewResult Combine(Memory<GetConflictsPreviewResult> results)
            {
                var totalResults = 0L;
                foreach (var conflictResult in results.Span)
                    totalResults += conflictResult.TotalResults;
                
                var final = new GetConflictsPreviewResult
                {
                    Results = new List<GetConflictsPreviewResult.ConflictPreview>(),
                    TotalResults = totalResults
                };

                var pageSize = _token.PageSize;
                foreach (var res in _handler.DatabaseContext.Streaming.CombinedResults(results, r => r.Results, ConflictsLastModifiedComparer.Instance))
                {
                    final.Results.Add(res.Item);
                    pageSize--;

                    if (pageSize <= 0)
                        break;

                    var shard = res.Shard;
                    _token.Pages[shard].Start = (int)res.Item.ScannedResults;
                }

                return final;
            }

            public RavenCommand<GetConflictsPreviewResult> CreateCommandForShard(int shardNumber) => new GetConflictsOperation.GetConflictsCommand(_token.Pages[shardNumber].Start, _token.PageSize);
        }

        public class ConflictsLastModifiedComparer : Comparer<ShardStreamItem<GetConflictsPreviewResult.ConflictPreview>>
        {
            public override int Compare(ShardStreamItem<GetConflictsPreviewResult.ConflictPreview> x,
                ShardStreamItem<GetConflictsPreviewResult.ConflictPreview> y)
            {
                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return ConflictsPreviewComparer.Instance.Compare(x.Item, y.Item);
            }

            public static ConflictsLastModifiedComparer Instance = new();
        }


    }
}
