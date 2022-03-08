using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class ConfigurationHandlerProcessorForGetStudioConfiguration : AbstractStudioConfigurationHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;

    public ConfigurationHandlerProcessorForGetStudioConfiguration(DatabaseRequestHandler requestHandler, [NotNull] DocumentDatabase database)
        : base(requestHandler, database.DocumentsStorage.ContextPool)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override StudioConfiguration GetStudioConfiguration()
    {
        return _database.StudioConfiguration;
    }
}
