using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForPostpone<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForPostpone([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();

    public override ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetStringQueryString("id");
        var timeInSec = RequestHandler.GetLongQueryString("timeInSec");

        var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
        var notificationCenter = GetNotificationCenter();
        notificationCenter.Postpone(id, until);

        RequestHandler.NoContentStatus();
        return ValueTask.CompletedTask;
    }
}
