using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.NotificationCenter.Commands;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal abstract class AbstractDatabaseNotificationCenterHandlerProcessorForDismiss<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDatabaseNotificationCenterHandlerProcessorForDismiss([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var id = GetNotificationId();
        var forever = GetForever();

        return new DismissNotificationCommand(id, forever, nodeTag);
    }

    protected abstract AbstractDatabaseNotificationCenter GetNotificationCenter();

    protected string GetNotificationId() => RequestHandler.GetStringQueryString("id");

    protected bool GetForever() => RequestHandler.GetBoolValueQueryString("forever", required: false) ?? false;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var id = GetNotificationId();
        var forever = GetForever();

        var notificationCenter = GetNotificationCenter();

        if (forever)
            notificationCenter.Postpone(id, DateTime.MaxValue);
        else
            notificationCenter.Dismiss(id);

        return ValueTask.CompletedTask;
    }
}
