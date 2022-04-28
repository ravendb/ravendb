using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal class AdminIndexHandlerProcessorForStop : AbstractAdminIndexHandlerProcessorForStop<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForStop([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask ExecuteForCurrentNodeAsync()
    {
        (string type, string name) = GetParameters();

        if (type == null && name == null)
        {
            RequestHandler.Database.IndexStore.StopIndexing();
            return ValueTask.CompletedTask;
        }

        if (type != null)
        {
            if (string.Equals(type, "map", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StopMapIndexes();
            }
            else if (string.Equals(type, "map-reduce", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StopMapReduceIndexes();
            }

            return ValueTask.CompletedTask;
        }

        RequestHandler.Database.IndexStore.StopIndex(name);
        return ValueTask.CompletedTask;
    }

    protected override Task ExecuteForRemoteNodeAsync(RavenCommand command) => RequestHandler.ExecuteRemoteAsync(command);
}
