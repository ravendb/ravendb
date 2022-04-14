using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Operations;
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

        public ShardedReplicationHandlerProcessorForGetConflicts([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task<GetConflictsPreviewResult> GetConflictsPreviewAsync(TransactionOperationContext context, long start, int pageSize)
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
                var span = results.Span;
                var final = new GetConflictsPreviewResult();

                final.Results = _handler.DatabaseContext.Streaming.PagedShardedItem(
                    results,
                    selector: r => r.Results,
                    comparer: ConflictsLastModifiedComparer.Instance,
                    _token).ToArray();

                foreach (var s in span)
                {
                    final.TotalResults += s.TotalResults;
                }

                return final;
            }

            public RavenCommand<GetConflictsPreviewResult> CreateCommandForShard(int shard) => new GetConflictsOperation.GetConflictsCommand(_token.Pages[shard].Start, _token.PageSize);
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
