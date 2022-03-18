using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal class ConfigurationHandlerProcessorForGetStudioConfiguration : AbstractConfigurationHandlerProcessorForGetStudioConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;

    public ConfigurationHandlerProcessorForGetStudioConfiguration([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
        _database = requestHandler.Database;
    }

    protected override StudioConfiguration GetStudioConfiguration()
    {
        return _database.StudioConfiguration;
    }
}
