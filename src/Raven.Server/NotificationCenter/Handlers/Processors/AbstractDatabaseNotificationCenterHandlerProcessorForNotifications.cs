using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Notifications;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForNotifications<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForNotifications([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();
    
    protected override RavenCommand<BlittableJsonReaderObject> CreateCommandForNode(string nodeTag)
    {
        return new GetDatabaseNotificationsOperation.GetDatabaseNotificationsCommand(nodeTag);
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var notificationCenter = GetNotificationCenter();
        
        var postponed = RequestHandler.GetBoolValueQueryString("postponed", required: false) ?? true;
        var type = RequestHandler.GetStringQueryString("type", required: false);
        var start = RequestHandler.GetIntValueQueryString("pageStart", required: false) ?? 0;
        var pageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;
        
        var filter = NotificationCenterHelper.GetAndEnsureValidTypeParameters(type);
        var shouldFilter = type != null;
        
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            using (notificationCenter.GetStored(out var storedNotifications, postponed))
            {
                var filteredNotifications = shouldFilter ? NotificationCenterHelper.FilterNotifications(storedNotifications, filter) : storedNotifications;

                writer.WriteNotifications(filteredNotifications, pageSize, start);
            }
        }
    }
}

