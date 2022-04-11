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
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetConflicts : AbstractReplicationHandlerProcessorForGetConflicts<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        private ShardedPagingContinuation _continuationToken;

        public ShardedReplicationHandlerProcessorForGetConflicts([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task<GetConflictsResultByEtag> GetConflictsByEtagAsync(TransactionOperationContext context, long etag, int pageSize)
        {
            _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, (int)etag, pageSize);

            var op = new ShardedGetReplicationConflictsOperation(RequestHandler, _continuationToken);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            result.ContinuationToken = _continuationToken.ToBase64(context);

            return result;
        }

        protected override async Task GetConflictsForDocumentAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetDocumentConflictsCommand(RequestHandler, documentId);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
        }

        internal readonly struct ShardedGetReplicationConflictsOperation : IShardedOperation<GetConflictsResultByEtag>
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly ShardedPagingContinuation _token;

            public ShardedGetReplicationConflictsOperation(ShardedDatabaseRequestHandler handler, ShardedPagingContinuation continuationToken)
            {
                _handler = handler;
                _token = continuationToken;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;

            public GetConflictsResultByEtag Combine(Memory<GetConflictsResultByEtag> results)
            {
                var span = results.Span;
                var final = new GetConflictsResultByEtag();

                final.Results = _handler.DatabaseContext.Streaming.PagedShardedItem(
                    results,
                    selector: r => r.Results,
                    Comparer<ShardStreamItem<GetConflictsResultByEtag.ResultByEtag>>.Default,
                    _token).ToArray();

                foreach (var s in span)
                {
                    final.TotalResults += s.TotalResults;
                }

                return final;
            }

            public RavenCommand<GetConflictsResultByEtag> CreateCommandForShard(int shard) => new GetConflictsByEtagOperation.GetConflictsByEtagCommand(_token.Pages[shard].Start, _token.PageSize);
        }
    }
}
