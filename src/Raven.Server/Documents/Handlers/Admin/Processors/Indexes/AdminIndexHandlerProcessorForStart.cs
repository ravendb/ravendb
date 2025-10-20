using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal sealed class AdminIndexHandlerProcessorForStart : AbstractAdminIndexHandlerProcessorForStart<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForStart([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        (string type, string name) = GetParameters();

        if (type == null && name == null)
        {
            RequestHandler.Database.IndexStore.StartIndexing();

            if (LoggingSource.AuditLog.IsInfoEnabled)
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Started all indexing.");

            return ValueTask.CompletedTask;
        }

        if (type != null)
        {
            if (string.Equals(type, "map", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StartMapIndexes();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Started all Map indexes.");
            }
            else if (string.Equals(type, "map-reduce", StringComparison.OrdinalIgnoreCase))
            {
                RequestHandler.Database.IndexStore.StartMapReduceIndexes();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", "Started all Map-Reduce indexes.");
            }

            return ValueTask.CompletedTask;
        }

        RequestHandler.Database.IndexStore.StartIndex(name);

        if (LoggingSource.AuditLog.IsInfoEnabled)
            RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", $"Started indexing for index '{name}'.");

        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
