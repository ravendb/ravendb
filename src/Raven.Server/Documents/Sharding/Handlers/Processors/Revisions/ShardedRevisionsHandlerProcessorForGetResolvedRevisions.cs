using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetResolvedRevisions : AbstractRevisionsHandlerProcessorForGetResolvedRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForGetResolvedRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetResolvedRevisionsAndWriteAsync(TransactionOperationContext context, DateTime since, int take, CancellationToken token)
        {
            var op = new ShardedGetResolvedRevisionsOperation(context, RequestHandler.HttpContext, since, take);
            var revisions = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);

            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                await writer.WriteArrayAsync(nameof(ResolvedRevisions.Results), revisions);
                writer.WriteEndObject();
            }
        }
    }

    internal readonly struct ShardedGetResolvedRevisionsOperation : IShardedOperation<ResolvedRevisions, List<BlittableJsonReaderObject>>
    {
        private readonly JsonOperationContext _context;
        private readonly HttpContext _httpContext;
        private readonly DateTime _since;
        private readonly int _take;

        public ShardedGetResolvedRevisionsOperation(JsonOperationContext context, HttpContext httpContext, DateTime since, int take)
        {
            _context = context;
            _httpContext = httpContext;
            _since = since;
            _take = take;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public List<BlittableJsonReaderObject> Combine(Memory<ResolvedRevisions> results)
        {
            var combined = new List<BlittableJsonReaderObject>();

            foreach (var revisions in results.Span)
            {
                if(revisions == null)
                    continue;
                
                foreach (BlittableJsonReaderObject revision in revisions.Results)
                {
                    combined.Add(revision.Clone(_context));
                }
            }

            return combined;
        }

        public RavenCommand<ResolvedRevisions> CreateCommandForShard(int shardNumber) => new GetResolvedRevisionsCommand(_since, _take);
    }
}
