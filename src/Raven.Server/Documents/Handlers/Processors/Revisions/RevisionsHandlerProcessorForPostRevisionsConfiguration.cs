using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.Revisions;

internal class RevisionsHandlerProcessorForPostRevisionsConfiguration : AbstractRevisionsHandlerProcessorForPostRevisionsConfiguration<DatabaseRequestHandler>
{
    public RevisionsHandlerProcessorForPostRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
    }
}
