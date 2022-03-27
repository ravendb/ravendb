using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.System.Processors.Databases;

internal class AdminDatabasesHandlerProcessorForGetDatabaseRecord : AbstractHandlerProcessorForGetDatabaseRecord<ServerRequestHandler, TransactionOperationContext>
{
    public AdminDatabasesHandlerProcessorForGetDatabaseRecord([NotNull] ServerRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ServerStore.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
}
