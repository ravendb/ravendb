using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.NotificationCenter.Commands;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForPostpone<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForPostpone([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var id = GetNotificationId();
        var timeInSec = GetTimeInSec();

        return new PostponeNotificationCommand(id, timeInSec, nodeTag);
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();

    protected string GetNotificationId() => RequestHandler.GetStringQueryString("id");

    protected long GetTimeInSec() => RequestHandler.GetLongQueryString("timeInSec");

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var id = GetNotificationId();
        var timeInSec = GetTimeInSec();

        var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
        var notificationCenter = GetNotificationCenter();
        notificationCenter.Postpone(id, until);

        return ValueTask.CompletedTask;
    }
}
