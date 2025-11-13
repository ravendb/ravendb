using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal sealed class AdminIndexHandlerProcessorForStop : AbstractAdminIndexHandlerProcessorForStop<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForStop([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        (string type, string name) = GetParameters();

        if (type == null && name == null)
        {
            RequestHandler.Database.IndexStore.StopIndexing();

            if (LoggingSource.AuditLog.IsInfoEnabled)
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Paused all indexing until restart.");

            return ValueTask.CompletedTask;
        }

        if (type != null)
        {
            if (string.Equals(type, "map", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StopMapIndexes();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Paused all Map indexes until restart.");
            }
            else if (string.Equals(type, "map-reduce", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StopMapReduceIndexes();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Paused all Map-Reduce indexes until restart.");
            }

            return ValueTask.CompletedTask;
        }

        RequestHandler.Database.IndexStore.StopIndex(name);

        if (LoggingSource.AuditLog.IsInfoEnabled)
            RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", $"Paused index '{name}' until restart.");

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
