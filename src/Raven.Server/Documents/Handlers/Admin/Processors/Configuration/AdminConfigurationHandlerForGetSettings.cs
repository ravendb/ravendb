using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerForGetSettings : AbstractAdminConfigurationHandlerForGetSettings<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerForGetSettings([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.Database.Configuration;
}
