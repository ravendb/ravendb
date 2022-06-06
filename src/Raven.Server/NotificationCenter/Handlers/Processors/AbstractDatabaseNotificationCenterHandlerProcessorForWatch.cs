using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForWatch<TRequestHandler, TOperationContext, TOperation> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperation : AbstractOperation, new()
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForWatch([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();

    protected abstract AbstractOperations<TOperation> GetOperations();

    protected abstract CancellationToken GetShutdownToken();

    public override async ValueTask ExecuteAsync()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        {
            var notificationCenter = GetNotificationCenter();
            var operations = GetOperations();
            var shutdownToken = GetShutdownToken();

            using (var writer = new NotificationCenterWebSocketWriter<TOperationContext>(webSocket, notificationCenter, ContextPool, shutdownToken))
            {
                using (notificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
                {
                    foreach (var alert in storedNotifications)
                    {
                        await writer.WriteToWebSocket(alert.Json);
                    }
                }

                foreach (var operation in operations.GetActive().OrderBy(x => x.Description.StartTime))
                {
                    var action = OperationChanged.Create(RequestHandler.DatabaseName, operation.Id, operation.Description, operation.State, operation.Killable);

                    await writer.WriteToWebSocket(action.ToJson());
                }
                writer.AfterTrackActionsRegistration = ServerStore.NotifyAboutClusterTopologyAndConnectivityChanges;
                await writer.WriteNotifications(null);
            }
        }
    }
}
