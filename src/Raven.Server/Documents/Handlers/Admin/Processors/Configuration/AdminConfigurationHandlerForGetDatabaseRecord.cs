using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal sealed class AdminConfigurationHandlerForGetDatabaseRecord : AbstractHandlerDatabaseProcessorForGetDatabaseRecord<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminConfigurationHandlerForGetDatabaseRecord([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
