using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal class ConfigurationHandlerProcessorForGetClientConfiguration : AbstractConfigurationHandlerProcessorForGetClientConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public ConfigurationHandlerProcessorForGetClientConfiguration([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override ClientConfiguration GetDatabaseClientConfiguration() => RequestHandler.Database.ClientConfiguration;
}
