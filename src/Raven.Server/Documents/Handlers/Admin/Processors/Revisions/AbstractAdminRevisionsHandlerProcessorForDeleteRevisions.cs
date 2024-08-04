using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected abstract Task<long> DeleteRevisionsAsync(DeleteRevisionsOperation.Parameters parameters,
            OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            DeleteRevisionsOperation.Parameters parameters;
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "admin/revisions/delete");
                parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "DELETE", $"Delete Revisions ,parameters: {json}");
                }
            }

            parameters.Validate();

            var deletedCount = 0L;
            using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken())
            {
                deletedCount = await DeleteRevisionsAsync(parameters, token);
            }

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(DeleteRevisionsOperation.Result.TotalDeletes));
                writer.WriteInteger(deletedCount);

                writer.WriteEndObject();
            }
        }

    }
}
