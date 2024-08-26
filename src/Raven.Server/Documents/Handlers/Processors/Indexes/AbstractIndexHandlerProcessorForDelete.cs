using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract AbstractIndexDeleteController GetIndexDeleteProcessor();

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();

        if (RavenLogManager.Instance.IsAuditEnabled)
        {
           RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "DELETE", $"Index '{name}'");
        }

        var processor = GetIndexDeleteProcessor();

        var statusCode = await processor.TryDeleteIndexIfExistsAsync(name, RequestHandler.GetRaftRequestIdFromQuery())
             ? HttpStatusCode.NoContent
             : HttpStatusCode.NotFound;

        RequestHandler.NoContentStatus(statusCode);
    }

    private string GetName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }
}
