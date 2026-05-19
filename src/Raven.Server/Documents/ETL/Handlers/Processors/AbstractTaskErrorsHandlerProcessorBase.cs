using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractTaskErrorsHandlerProcessorBase<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<TaskErrors[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractTaskErrorsHandlerProcessorBase([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected static TaskErrors BuildTaskErrors(
        string taskName,
        EtlProcess process,
        IEnumerable<TaskProcessErrorTableValue> processErrors,
        IEnumerable<TaskItemErrorTableValue> itemErrors)
    {
        return new TaskErrors
        {
            TaskName = taskName,
            EtlType = process?.EtlType,
            EtlSubType = process?.EtlSubType,
            ProcessErrors = processErrors.Select(x => x.ToTaskProcessError()).ToArray(),
            ItemErrors = itemErrors.Select(x => x.ToTaskItemError()).ToArray()
        };
    }

    protected async ValueTask WriteTaskErrorsResponseAsync(TaskErrorsResponse response, string debugName)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(TaskErrorsResponse.NodeTag));
                writer.WriteString(response.NodeTag);
                writer.WriteComma();

                writer.WritePropertyName(nameof(TaskErrorsResponse.ShardNumber));
                if (response.ShardNumber != null)
                    writer.WriteInteger(response.ShardNumber.Value);
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WriteArray(context, nameof(TaskErrorsResponse.Results), response.Results, (w, c, errors) => w.WriteObject(c.ReadObject(errors.ToJson(), debugName)));
                writer.WriteEndObject();
            }
        }
    }
}
