using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers;

internal class AnalyzersHandlerProcessorForGet<TOperationContext> : AbstractHandlerProcessor<RequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
{
    private readonly string _databaseName;

    public AnalyzersHandlerProcessorForGet([NotNull] RequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool, [NotNull] string databaseName)
        : base(requestHandler, contextPool)
    {
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
    }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            Dictionary<string, AnalyzerDefinition> analyzers;
            using (context.OpenReadTransaction())
            {
                var rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, _databaseName);
                analyzers = rawRecord?.Analyzers;
            }

            analyzers ??= new Dictionary<string, AnalyzerDefinition>();

            var namesOnly = RequestHandler.GetBoolValueQueryString("namesOnly", false) ?? false;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, "Analyzers", analyzers.Values, (w, c, analyzer) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(AnalyzerDefinition.Name));
                    w.WriteString(analyzer.Name);

                    if (namesOnly == false)
                    {
                        w.WriteComma();
                        w.WritePropertyName(nameof(AnalyzerDefinition.Code));
                        w.WriteString(analyzer.Code);
                    }

                    w.WriteEndObject();
                });

                writer.WriteEndObject();
            }
        }
    }
}
