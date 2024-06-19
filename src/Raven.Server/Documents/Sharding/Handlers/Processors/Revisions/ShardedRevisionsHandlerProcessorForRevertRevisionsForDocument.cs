using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForRevertRevisionsForDocument : AbstractRevisionsHandlerProcessorForRevertRevisionsForDocument<
        ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForRevertRevisionsForDocument([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task RevertDocuments(Dictionary<string, string> idToChangeVector, OperationCancelToken token)
        {
            var shardsToDocs = new Dictionary<long, Dictionary<string, string>>();
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                foreach (var (id, cv) in idToChangeVector)
                {
                    var config = RequestHandler.DatabaseContext.DatabaseRecord.Sharding;
                    var shardNumber = ShardHelper.GetShardNumberFor(config, context, id);

                    shardsToDocs[shardNumber] ??= new Dictionary<string, string>();
                    shardsToDocs[shardNumber].Add(id, cv);
                }
            }

            var op = new RevertDocumentsToRevisionsOperation(shardsToDocs);
            await RequestHandler.ShardExecutor.ExecuteParallelForAllThrowAggregatedFailure(op, token.Token);
        }
    }

    public readonly struct RevertDocumentsToRevisionsOperation : IShardedOperation
    {
        private readonly Dictionary<long, Dictionary<string, string>> _shardsToDocs;

        public RevertDocumentsToRevisionsOperation(Dictionary<long, Dictionary<string, string>> shardsToDocs)
        {
            _shardsToDocs = shardsToDocs;
        }

        public HttpRequest HttpRequest => null;

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            if (_shardsToDocs.Keys.Contains(shardNumber) == false)
                return null;

            return new RevertDocumentsToRevisionsCommand(_shardsToDocs[shardNumber]);
        }

        public sealed class RevertDocumentsToRevisionsCommand : RavenCommand
        {
            private readonly Dictionary<string, string> _idToChangeVector;

            public RevertDocumentsToRevisionsCommand(Dictionary<string, string> idToChangeVector)
            {
                _idToChangeVector = idToChangeVector;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions/revert/document";

                var request = new RevertDocumentsToRevisionsRequest { IdToChangeVector = _idToChangeVector };

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                            await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(request, ctx)).ConfigureAwait(false),
                        DocumentConventions.Default)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();
            }

            public override bool IsReadRequest => true;
        }
    }
}
