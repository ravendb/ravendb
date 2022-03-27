using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerProcessorForPutSettings : AbstractAdminConfigurationHandlerProcessorForPutSettings<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForPutSettings(DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
