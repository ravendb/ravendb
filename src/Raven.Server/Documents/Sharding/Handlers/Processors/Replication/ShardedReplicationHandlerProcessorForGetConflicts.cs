using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetConflicts : AbstractReplicationHandlerProcessorForGetConflicts<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedReplicationHandlerProcessorForGetConflicts([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task GetConflictsByEtagAsync(TransactionOperationContext context, long etag)
        {
            var op = new ShardedGetReplicationConflictsOperation(RequestHandler.HttpContext, context, etag);
            var conflicts = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            await WriteConflictsByEtag(context, conflicts, conflicts.Length);
        }

        protected override async Task GetConflictsForDocumentAsync(TransactionOperationContext context, string documentId)
        {
            var cmd = new GetDocumentConflictsCommand(RequestHandler, documentId);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
        }

        private async Task WriteConflictsByEtag(JsonOperationContext context, BlittableJsonReaderObject[] conflicts, long totalResults)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["TotalResults"] = totalResults,
                    [nameof(GetConflictsResult.Results)] = conflicts
                });
            }
        }

        internal readonly struct ShardedGetReplicationConflictsOperation : IShardedOperation<BlittableArrayResult, BlittableJsonReaderObject[]>
        {
            private readonly HttpContext _httpContext;
            private readonly TransactionOperationContext _context;
            private readonly long _etag;

            public ShardedGetReplicationConflictsOperation(HttpContext httpContext, TransactionOperationContext context, long etag)
            {
                _httpContext = httpContext;
                _context = context;
                _etag = etag;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public BlittableJsonReaderObject[] Combine(Memory<BlittableArrayResult> results)
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

                var combined = new BlittableJsonReaderObject[len];

                int index = 0;
                foreach (var s in span)
                {
                    if (s == null || s.Results.Length == 0)
                        continue;

                    foreach (var result in s.Results)
                    {
                        if (result is BlittableJsonReaderObject conflict)
                        {
                            combined[index] = conflict.Clone(_context);
                            index++;
                            break;
                        }
                    }
                }

                return combined;
            }

            public RavenCommand<BlittableArrayResult> CreateCommandForShard(int shard) => new GetReplicationConflictsOperation.GetReplicationConflictsCommand(etag: _etag);
        }
    }
}
