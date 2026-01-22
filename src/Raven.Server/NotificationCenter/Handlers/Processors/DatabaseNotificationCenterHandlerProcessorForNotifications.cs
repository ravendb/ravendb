using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Notifications;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal sealed class DatabaseNotificationCenterHandlerProcessorForNotifications : AbstractDatabaseNotificationCenterHandlerProcessorForNotifications<DatabaseRequestHandler, DocumentsOperationContext>
{
    public DatabaseNotificationCenterHandlerProcessorForNotifications([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }
    
    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.Database.NotificationCenter;

    protected override bool SupportsCurrentNode => true;
    
    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
