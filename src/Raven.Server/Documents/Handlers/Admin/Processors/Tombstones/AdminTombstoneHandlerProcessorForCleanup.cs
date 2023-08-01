using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;

internal sealed class AdminTombstoneHandlerProcessorForCleanup : AbstractAdminTombstoneHandlerProcessorForCleanup<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminTombstoneHandlerProcessorForCleanup([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var count = await RequestHandler.Database.TombstoneCleaner.ExecuteCleanup();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Value");
                writer.WriteInteger(count);

                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<CleanupTombstonesCommand.Response> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
