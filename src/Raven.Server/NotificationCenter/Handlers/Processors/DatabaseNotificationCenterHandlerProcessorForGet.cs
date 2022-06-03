using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal class DatabaseNotificationCenterHandlerProcessorForGet : AbstractDatabaseNotificationCenterHandlerProcessorForGet<DatabaseRequestHandler, DocumentsOperationContext, Operation>
{
    public DatabaseNotificationCenterHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.Database.NotificationCenter;

    protected override AbstractOperations<Operation> GetOperations() => RequestHandler.Database.Operations;

    protected override CancellationToken GetShutdownToken() => RequestHandler.Database.DatabaseShutdown;
}
