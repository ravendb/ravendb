using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal class ConfigurationHandlerProcessorForGetStudioConfiguration : AbstractConfigurationHandlerProcessorForGetStudioConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{

    public ConfigurationHandlerProcessorForGetStudioConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override StudioConfiguration GetStudioConfiguration() => RequestHandler.Database.StudioConfiguration;
}
