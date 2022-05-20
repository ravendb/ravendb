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

internal class ShardedAttachmentHandlerProcessorForGetHashCount : AbstractAttachmentHandlerProcessorForGetHashCount<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForGetHashCount([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<GetAttachmentHashCountCommand.Response> GetResponseAsync(TransactionOperationContext context, string hash)
    {
        return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedAttachmentExistsOperation(HttpContext.Request, hash));
    }

    internal readonly struct ShardedAttachmentExistsOperation : IShardedOperation<GetAttachmentHashCountCommand.Response>
    {
        private readonly string _hash;

        public HttpRequest HttpRequest { get; }

        public ShardedAttachmentExistsOperation([NotNull] HttpRequest httpRequest, [NotNull] string hash)
        {
            _hash = hash ?? throw new ArgumentNullException(nameof(hash));
            HttpRequest = httpRequest ?? throw new ArgumentNullException(nameof(httpRequest));
        }

        public GetAttachmentHashCountCommand.Response Combine(Memory<GetAttachmentHashCountCommand.Response> results)
        {
            var response = new GetAttachmentHashCountCommand.Response
            {
                Hash = _hash
            };

            var responses = results.Span;
            foreach (var r in responses)
                response.Count += r.Count;

            return response;
        }

        public RavenCommand<GetAttachmentHashCountCommand.Response> CreateCommandForShard(int shardNumber) => new GetAttachmentHashCountCommand(_hash);
    }
}
