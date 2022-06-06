using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForDismiss<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForDismiss([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();

    public override ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetStringQueryString("id");
        var forever = RequestHandler.GetBoolValueQueryString("forever", required: false);

        var notificationCenter = GetNotificationCenter();

        if (forever == true)
            notificationCenter.Postpone(id, DateTime.MaxValue);
        else
            notificationCenter.Dismiss(id);

        RequestHandler.NoContentStatus();
        return ValueTask.CompletedTask;
    }
}
