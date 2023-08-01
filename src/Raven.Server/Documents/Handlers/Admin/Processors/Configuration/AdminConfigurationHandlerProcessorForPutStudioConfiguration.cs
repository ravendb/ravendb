using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration
{
    internal sealed class AdminConfigurationHandlerProcessorForPutStudioConfiguration : AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminConfigurationHandlerProcessorForPutStudioConfiguration(DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
        }
    }
}
