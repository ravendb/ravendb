using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal class DatabaseNotificationCenterHandlerProcessorForDismiss : AbstractDatabaseNotificationCenterHandlerProcessorForDismiss<DatabaseRequestHandler, DocumentsOperationContext>
{
    public DatabaseNotificationCenterHandlerProcessorForDismiss([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.Database.NotificationCenter;
}
