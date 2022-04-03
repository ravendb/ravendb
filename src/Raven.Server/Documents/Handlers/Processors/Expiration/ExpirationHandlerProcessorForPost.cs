using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.Expiration
{
    internal class ExpirationHandlerProcessorForPost : AbstractExpirationHandlerProcessorForPost<DatabaseRequestHandler>
    {
        public ExpirationHandlerProcessorForPost([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
        }

    }
}
