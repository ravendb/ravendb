using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerProcessorForPutClientConfiguration : AbstractAdminConfigurationHandlerProcessorForPutClientConfiguration<DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForPutClientConfiguration(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
