using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerProcessorForPutClientConfiguration : AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForPutClientConfiguration(DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
