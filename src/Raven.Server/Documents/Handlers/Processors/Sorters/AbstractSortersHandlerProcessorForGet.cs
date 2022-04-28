using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Sorters;

internal abstract class AbstractSortersHandlerProcessorForGet<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractSortersHandlerProcessorForGet([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler) 
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            Dictionary<string, SorterDefinition> sorters;
            using (context.OpenReadTransaction())
            {
                var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName);
                sorters = rawRecord?.Sorters;
            }

            if (sorters == null)
            {
                sorters = new Dictionary<string, SorterDefinition>();
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, "Sorters", sorters.Values, (w, c, sorter) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(SorterDefinition.Name));
                    w.WriteString(sorter.Name);
                    w.WriteComma();

                    w.WritePropertyName(nameof(SorterDefinition.Code));
                    w.WriteString(sorter.Code);

                    w.WriteEndObject();
                });

                writer.WriteEndObject();
            }
        }
    }
}
