using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetIndexHistory<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{

    public IndexHandlerProcessorForGetIndexHistory([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();

        List<IndexHistoryEntry> history;
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
        {
            var indexesHistory = rawRecord.IndexesHistory;
            if (indexesHistory.TryGetValue(name, out history) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(IndexHandler.IndexHistoryResult.Index));
            writer.WriteString(name);
            writer.WriteComma();

            if (history.Count == 0)
            {
                writer.WriteStartArray();
                writer.WriteEndArray();
                writer.WriteEndObject();
                return;
            }

            writer.WriteArray(context, nameof(IndexHandler.IndexHistoryResult.History), history, (w, c, entry) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(IndexHistoryEntry.Definition));
                w.WriteIndexDefinition(c, entry.Definition);
                w.WriteComma();

                w.WritePropertyName(nameof(IndexHistoryEntry.CreatedAt));
                w.WriteDateTime(entry.CreatedAt, isUtc: true);
                w.WriteComma();

                w.WritePropertyName(nameof(IndexHistoryEntry.Source));
                w.WriteString(entry.Source);
                w.WriteComma();

                var first = true;
                w.WritePropertyName(nameof(IndexHistoryEntry.RollingDeployment));
                w.WriteStartObject();
                foreach (var rollingIndexDeployment in entry.RollingDeployment)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        w.WriteComma();
                    }
                    w.WritePropertyName(rollingIndexDeployment.Key);
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(rollingIndexDeployment.Value.CreatedAt));
                    w.WriteDateTime(rollingIndexDeployment.Value.CreatedAt, isUtc: true);
                    w.WriteComma();

                    if (rollingIndexDeployment.Value.StartedAt.HasValue)
                    {
                        w.WritePropertyName(nameof(rollingIndexDeployment.Value.StartedAt));
                        w.WriteDateTime((DateTime)rollingIndexDeployment.Value.StartedAt, isUtc: true);
                        w.WriteComma();
                    }

                    if (rollingIndexDeployment.Value.FinishedAt.HasValue)
                    {
                        w.WritePropertyName(nameof(rollingIndexDeployment.Value.FinishedAt));
                        w.WriteDateTime((DateTime)rollingIndexDeployment.Value.FinishedAt, isUtc: true);
                        w.WriteComma();
                    }

                    w.WritePropertyName(nameof(rollingIndexDeployment.Value.State));
                    w.WriteString(rollingIndexDeployment.Value.State.ToString());

                    w.WriteEndObject();
                }
                w.WriteEndObject();
                w.WriteEndObject();
            });

            writer.WriteEndObject();
        }
    }

    private string GetName() => RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
}
