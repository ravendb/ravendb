using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal class AdminAnalyzersHandlerProcessorForDelete : AbstractAdminAnalyzersHandlerProcessorForDelete<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminAnalyzersHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
        }
    }
}
