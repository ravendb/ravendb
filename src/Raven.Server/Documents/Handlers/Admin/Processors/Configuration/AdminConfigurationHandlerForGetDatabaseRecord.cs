using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerForGetDatabaseRecord : AbstractHandlerProcessorForGetDatabaseRecord<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerForGetDatabaseRecord([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;
}
