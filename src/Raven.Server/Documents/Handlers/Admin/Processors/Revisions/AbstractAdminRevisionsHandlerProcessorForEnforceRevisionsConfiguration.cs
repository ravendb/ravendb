using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Revisions;
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

        protected abstract void ScheduleEnforceConfigurationOperation(long operationId, bool includeForceCreated, HashSet<string> collections, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken();
            var operationId = RequestHandler.GetLongQueryString("operationId", false) ?? GetNextOperationId();

            EnforceRevisionsConfigurationRequest configuration;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/revert");
                configuration = JsonDeserializationServer.EnforceRevisionsConfiguration(json);
            }

            HashSet<string> collections = configuration.Collections?.Length > 0 ? new HashSet<string>(configuration.Collections, StringComparer.OrdinalIgnoreCase) : null;

            ScheduleEnforceConfigurationOperation(operationId, configuration.IncludeForceCreated, collections, token);

            using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }
}
