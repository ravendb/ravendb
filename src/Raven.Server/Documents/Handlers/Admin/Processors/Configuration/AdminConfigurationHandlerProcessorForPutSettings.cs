using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal sealed class AdminConfigurationHandlerProcessorForPutSettings : AbstractAdminConfigurationHandlerProcessorForPutSettings<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForPutSettings(DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}
