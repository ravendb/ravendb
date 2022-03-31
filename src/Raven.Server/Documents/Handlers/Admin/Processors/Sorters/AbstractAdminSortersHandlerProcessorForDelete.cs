using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal abstract class AbstractAdminSortersHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAdminSortersHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

        var databaseName = GetDatabaseName();

        if (LoggingSource.AuditLog.IsInfoEnabled)
        {
            var clientCert = RequestHandler.GetCurrentCertificate();

            var auditLog = LoggingSource.AuditLog.GetLogger(databaseName, "Audit");
            auditLog.Info($"Sorter {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
        }

        var command = new DeleteSorterCommand(name, databaseName, RequestHandler.GetRaftRequestIdFromQuery());
        var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

        await WaitForIndexNotificationAsync(index);

        RequestHandler.NoContentStatus();
    }
}
