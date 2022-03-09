using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class ConfigurationHandlerProcessorForPutAdminConfiguration : AbstractConfigurationHandlerProcessorForPutAdminConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ConfigurationHandlerProcessorForPutAdminConfiguration(DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool) { }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.Database.Name;
        }
    }
}
