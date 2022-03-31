using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal class AdminSortersHandlerProcessorForDelete : AbstractAdminSortersHandlerProcessorForDelete<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminSortersHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
    }
}
