using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.Refresh;

internal class RefreshHandlerProcessorForPostRefreshConfiguration : AbstractRefreshHandlerProcessorForPostRefreshConfiguration<DatabaseRequestHandler>
{
    public RefreshHandlerProcessorForPostRefreshConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
    }
}
