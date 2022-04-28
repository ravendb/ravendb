using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration
{
    internal class AdminConfigurationHandlerProcessorForPutStudioConfiguration : AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<DocumentsOperationContext>
    {
        public AdminConfigurationHandlerProcessorForPutStudioConfiguration(DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
