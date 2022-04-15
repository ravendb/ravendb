using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForHead : AbstractDocumentHandlerProcessorForHead<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForHead([NotNull] ShardedDocumentHandler requestHandler, [NotNull] JsonContextPoolBase<TransactionOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override async ValueTask<(HttpStatusCode StatusCode, string ChangeVector)> GetStatusCodeAndChangeVector(string docId, TransactionOperationContext context)
    {
        var index = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

        var cmd = new ShardedHeadCommand(RequestHandler, Headers.IfNoneMatch);

        await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(cmd, index);

        return (cmd.StatusCode, cmd.Result);
    }
}
