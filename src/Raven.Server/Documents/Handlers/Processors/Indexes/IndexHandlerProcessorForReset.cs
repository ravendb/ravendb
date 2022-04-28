using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForReset : AbstractIndexHandlerProcessorForReset<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForReset([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask ExecuteForCurrentNodeAsync()
    {
        var name = GetName();
        RequestHandler.Database.IndexStore.ResetIndex(name);

        return ValueTask.CompletedTask;
    }

    protected override Task ExecuteForRemoteNodeAsync(RavenCommand command) => RequestHandler.ExecuteRemoteAsync(command);
}
