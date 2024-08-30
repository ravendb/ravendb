using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForWatch<TRequestHandler, TOperationContext, TOperation> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
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

    protected override string GetRemoteEndpointUrl(string databaseName) => $"/databases/{databaseName}/notification-center/watch";

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var notificationCenter = GetNotificationCenter();
        var operations = GetOperations();

        using (var writer = new NotificationCenterWebSocketWriter<TOperationContext>(webSocket, notificationCenter, ContextPool, token.Token))
        {
            using (notificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                foreach (var alert in storedNotifications)
                {
                    using (alert)
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
