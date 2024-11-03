using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal sealed class ShardedStudioCollectionsHandlerProcessorForRevisionsIds : AbstractStudioCollectionsHandlerProcessorForRevisionsIds<ShardedDatabaseRequestHandler,
        TransactionOperationContext>
    {
        private CombinedReadContinuationState _combinedReadState;

        public ShardedStudioCollectionsHandlerProcessorForRevisionsIds([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IDisposable OpenReadTransaction(TransactionOperationContext context) => null;

        protected override async Task WriteItemsAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer, CancellationToken token)
        {
            var op = new ShardedRevisionsIdsOperation(RequestHandler, Prefix, PageSize);
            var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);

            _combinedReadState = await result.Result.InitializeAsync(RequestHandler.DatabaseContext, RequestHandler.AbortRequestToken);
            var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context);

            var shardItems = RequestHandler.DatabaseContext.Streaming.PagedShardedStream(
                _combinedReadState,
                "Results",
                x => x,
                ShardedDatabaseContext.ShardedStreaming.RevisionIdComparer.Instance,
                continuationToken);

            writer.WriteStartArray();
            var first = true;
            await foreach (var item in shardItems)
            {
                if (first)
                    first = false;
                else
                    writer.WriteComma();

                var json = item.Item;

                if (json.TryGet(nameof(Document.Id), out string id) == false)
                    throw new InvalidOperationException("Revision does not contain 'Id' field.");

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(Document.Id));
                writer.WriteString(id);

                writer.WriteComma();

                writer.WritePropertyName(nameof(ShardStreamItem<BlittableJsonReaderObject>.ShardNumber));
                writer.WriteInteger(item.ShardNumber);

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }


        private readonly struct ShardedRevisionsIdsOperation : IShardedStreamableOperation
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly string _prefix;
            private readonly int _pageSize;

            public ShardedRevisionsIdsOperation(ShardedDatabaseRequestHandler handler, string prefix, int pageSize)
            {
                _handler = handler;
                _prefix = prefix;
                _pageSize = pageSize;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;

            public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber)
            {
                return new ShardedRevisionsIdsCommand(_prefix, _pageSize);
            }

            private sealed class ShardedRevisionsIdsCommand : RavenCommand<StreamResult>
            {
                private readonly string _prefix;
                private readonly int _pageSize;

                public ShardedRevisionsIdsCommand(string prefix, int pageSize)
                {
                    _prefix = prefix;
                    _pageSize = pageSize;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/studio/revisions/ids?prefix={_prefix}&{Web.RequestHandler.PageSizeParameter}={_pageSize}";

                    var message = new HttpRequestMessage { Method = HttpMethod.Get, };

                    return message;
                }

                public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response,
                    string url)
                {
                    var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);

                    Result = new StreamResult { Response = response, Stream = new StreamWithTimeout(responseStream) };

                    return ResponseDisposeHandling.Manually;
                }
            }

            public string ExpectedEtag { get; }

            public CombinedStreamResult CombineResults(Dictionary<int, ShardExecutionResult<StreamResult>> results)
            {
                return new CombinedStreamResult { Results = results };
            }
        }

    }
}
