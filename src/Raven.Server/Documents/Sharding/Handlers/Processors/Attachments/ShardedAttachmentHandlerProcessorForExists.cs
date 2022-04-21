using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;

internal class ShardedAttachmentHandlerProcessorForExists : AbstractAttachmentHandlerProcessorForExists<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForExists([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask<AttachmentExistsCommand.Response> GetResponseAsync(TransactionOperationContext context, string hash)
    {
        return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedAttachmentExistsOperation(HttpContext.Request, hash));
    }

    internal readonly struct ShardedAttachmentExistsOperation : IShardedOperation<AttachmentExistsCommand.Response>
    {
        private readonly string _hash;

        public HttpRequest HttpRequest { get; }

        public ShardedAttachmentExistsOperation([NotNull] HttpRequest httpRequest, [NotNull] string hash)
        {
            _hash = hash ?? throw new ArgumentNullException(nameof(hash));
            HttpRequest = httpRequest ?? throw new ArgumentNullException(nameof(httpRequest));
        }

        public AttachmentExistsCommand.Response Combine(Memory<AttachmentExistsCommand.Response> results)
        {
            var response = new AttachmentExistsCommand.Response
            {
                Hash = _hash
            };

            var responses = results.Span;
            foreach (var r in responses)
                response.Count += r.Count;

            return response;
        }

        public RavenCommand<AttachmentExistsCommand.Response> CreateCommandForShard(int shard) => new AttachmentExistsCommand(_hash);
    }
}
