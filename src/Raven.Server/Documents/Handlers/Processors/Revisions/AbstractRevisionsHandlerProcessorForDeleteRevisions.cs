using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;


namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForDeleteRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractRevisionsHandlerProcessorForDeleteRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract Task<long> DeleteRevisionsAsync(DeleteRevisionsRequest request, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            DeleteRevisionsRequest request;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/delete");

                request = JsonDeserializationServer.DeleteRevisions(json);
            }

            request.Validate();

            long deletedCount;

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                deletedCount = await DeleteRevisionsAsync(request, token);
            }

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                RequestHandler.LogAuditFor("Database", "DELETE",
                    $"{RequestHandler.DatabaseName} - {deletedCount} revisions of '{request.DocumentId}' had been deleted manually" +
                    (request.After.HasValue || request.Before.HasValue ? 
                        $", in the range {request.After?.ToString() ?? "start"} to {request.Before?.ToString() ?? "end"}" : string.Empty));
            }

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(DeleteRevisionsManuallyOperation.Result.TotalDeletes));
                writer.WriteInteger(deletedCount);

                writer.WriteEndObject();
            }
        }

    }
}
