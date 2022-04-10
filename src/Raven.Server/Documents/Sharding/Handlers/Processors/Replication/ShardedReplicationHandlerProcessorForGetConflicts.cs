using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetConflicts : AbstractReplicationHandlerProcessorForGetConflicts<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedReplicationHandlerProcessorForGetConflicts([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task<GetConflictsResultByEtag> GetConflictsByEtagAsync(TransactionOperationContext context, long etag)
        {
            var op = new ShardedGetReplicationConflictsOperation(RequestHandler.HttpContext, etag);
            var conflicts = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            return conflicts;
        }

        protected override async Task GetConflictsForDocumentAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetDocumentConflictsCommand(RequestHandler, documentId);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
        }

        internal readonly struct ShardedGetReplicationConflictsOperation : IShardedOperation<GetConflictsResultByEtag>
        {
            private readonly HttpContext _httpContext;
            private readonly long _etag;

            public ShardedGetReplicationConflictsOperation(HttpContext httpContext, long etag)
            {
                _httpContext = httpContext;
                _etag = etag;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public GetConflictsResultByEtag Combine(Memory<GetConflictsResultByEtag> results)
            {
                var span = results.Span;

                int len = 0;
                foreach (var s in span)
                {
                    if (s != null)
                        len += s.Results.Length;
                }

                if (len == 0)
                    return null;

                var conflicts = new List<GetConflictsResultByEtag.ResultByEtag>();
                long totalResults = 0;

                foreach (var s in span)
                {
                    totalResults += s.TotalResults;
                    foreach (var res in s.Results)
                    {
                        conflicts.Add(res);
                    }
                }

                return new GetConflictsResultByEtag { Results = conflicts.ToArray(), TotalResults = totalResults };
            }

            public RavenCommand<GetConflictsResultByEtag> CreateCommandForShard(int shard) => new GetConflictsByEtagOperation.GetConflictsByEtagCommand(etag: _etag);
        }
    }
}
