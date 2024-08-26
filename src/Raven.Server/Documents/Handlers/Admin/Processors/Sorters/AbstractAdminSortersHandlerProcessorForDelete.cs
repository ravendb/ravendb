using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands.Sorters;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal abstract class AbstractAdminSortersHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminSortersHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

        var databaseName = RequestHandler.DatabaseName;

        if (RavenLogManager.Instance.IsAuditEnabled)
        {
           RequestHandler.LogAuditFor(databaseName, "DELETE", $"Sorter '{name}'");
        }

        var command = new DeleteSorterCommand(name, databaseName, RequestHandler.GetRaftRequestIdFromQuery());
        var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

        await RequestHandler.WaitForIndexNotificationAsync(index);

        RequestHandler.NoContentStatus();
    }
}
