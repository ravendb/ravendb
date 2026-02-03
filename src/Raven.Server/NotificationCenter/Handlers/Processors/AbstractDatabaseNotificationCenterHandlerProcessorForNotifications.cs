using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;
using GetDatabaseNotificationsCommand = Raven.Server.Documents.Commands.Notifications.GetDatabaseNotificationsCommand;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForNotifications<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected bool Postponed { get; init; }
    protected string Type { get; init; }
    protected int Start { get; init; }
    protected int PageSize { get; init; }
    
    protected AbstractDatabaseNotificationCenterHandlerProcessorForNotifications([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
        Postponed = RequestHandler.GetBoolValueQueryString("postponed", required: false) ?? true; 
        Type = RequestHandler.GetStringQueryString("type", required: false);
        Start = RequestHandler.GetIntValueQueryString("pageStart", required: false) ?? 0;
        PageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();
    
    protected override RavenCommand<BlittableJsonReaderObject> CreateCommandForNode(string nodeTag)
    {
        return new GetDatabaseNotificationsCommand(Postponed, Type, Start, PageSize, nodeTag);
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var notificationCenter = GetNotificationCenter();
        
        var filter = NotificationCenterHelper.GetAndEnsureValidTypeParameters(Type);
        var shouldFilter = Type != null;
        
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            using (notificationCenter.GetStored(out var storedNotifications, Postponed))
            {
                var filteredNotifications = shouldFilter ? NotificationCenterHelper.FilterNotifications(storedNotifications, filter) : storedNotifications;

                writer.WriteNotifications(filteredNotifications, PageSize, Start);
            }
        }
    }
}

