using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerProcessorForGetSettings : AbstractAdminConfigurationHandlerProcessorForGetSettings<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerProcessorForGetSettings([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }


    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;
}
