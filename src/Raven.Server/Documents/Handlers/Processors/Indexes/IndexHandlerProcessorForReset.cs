using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForReset : AbstractIndexHandlerProcessorForReset<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForReset([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask ExecuteForCurrentNodeAsync()
    {
        var name = GetName();
        RequestHandler.Database.IndexStore.ResetIndex(name);

        return ValueTask.CompletedTask;
    }

    protected override Task ExecuteForRemoteNodeAsync(ProxyCommand command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
