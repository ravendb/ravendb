using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

namespace Raven.Server.Web.System.Processors.Databases;

internal class AdminDatabasesHandlerProcessorForGetDatabaseRecord : AbstractHandlerProcessorForGetDatabaseRecord<ServerRequestHandler>
{
    public AdminDatabasesHandlerProcessorForGetDatabaseRecord([NotNull] ServerRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string DatabaseName => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
}
