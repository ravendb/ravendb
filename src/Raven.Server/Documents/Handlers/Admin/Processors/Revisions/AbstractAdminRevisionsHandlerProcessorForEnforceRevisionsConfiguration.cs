using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract long GetNextOperationId();

        protected abstract void ScheduleEnforceConfigurationOperation(long operationId, EnforceRevisionsConfigurationOperation.Parameters parameters, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken();
            var operationId = RequestHandler.GetLongQueryString("operationId", false) ?? GetNextOperationId();

            EnforceRevisionsConfigurationOperation.Parameters parameters;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/revert");
                parameters = JsonDeserializationServer.Parameters.EnforceRevisionsConfigurationOperationParameters(json);
            }

            ScheduleEnforceConfigurationOperation(operationId, parameters, token);

            using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }
}
