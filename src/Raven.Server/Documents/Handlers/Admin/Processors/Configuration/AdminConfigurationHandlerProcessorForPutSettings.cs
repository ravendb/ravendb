using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerProcessorForPutSettings : AbstractAdminConfigurationHandlerProcessorForPutSettings<DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForPutSettings(DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }
}
