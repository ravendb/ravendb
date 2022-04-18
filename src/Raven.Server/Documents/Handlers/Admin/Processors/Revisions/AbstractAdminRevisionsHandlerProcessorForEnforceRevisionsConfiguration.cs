using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<OperationIdResults> AddOperation(long operationId, OperationCancelToken token);

        protected abstract OperationCancelToken CreateTimeLimitedOperationToken();

        public override async ValueTask ExecuteAsync()
        {
            var token = CreateTimeLimitedOperationToken();
            var operationId = RequestHandler.ServerStore.Operations.GetNextOperationId();

            var operations = await AddOperation(operationId, token);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(OperationIdResults.Results));
                writer.WriteStartArray();
                for (int i = 0; i < operations.Results.Count; i++)
                {
                    if(i!=0)
                        writer.WriteComma();

                    writer.WriteOperationIdAndNodeTag(context, operations.Results[i].OperationId, operations.Results[i].OperationNodeTag);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }
    }
}
