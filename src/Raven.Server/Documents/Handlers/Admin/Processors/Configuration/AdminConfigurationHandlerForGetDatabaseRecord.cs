using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal class AdminConfigurationHandlerForGetDatabaseRecord : AbstractHandlerDatabaseProcessorForGetDatabaseRecord<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerForGetDatabaseRecord([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
