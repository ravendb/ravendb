using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract AbstractIndexDeleteController GetIndexDeleteProcessor();

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();

        if (LoggingSource.AuditLog.IsInfoEnabled)
        {
            var clientCert = RequestHandler.GetCurrentCertificate();

            var auditLog = LoggingSource.AuditLog.GetLogger(GetDatabaseName(), "Audit");
            auditLog.Info($"Index {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
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
