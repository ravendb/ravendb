using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class AnalyzersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/analyzers", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                Dictionary<string, AnalyzerDefinition> analyzers;
                using (context.OpenReadTransaction())
                {
                    var rawRecord = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name);
                    analyzers = rawRecord?.Analyzers;
                }

                if (analyzers == null)
                {
                    analyzers = new Dictionary<string, AnalyzerDefinition>();
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Analyzers", analyzers.Values, (w, c, analyzer) =>
                    {
                        w.WriteStartObject();

                        w.WritePropertyName(nameof(AnalyzerDefinition.Name));
                        w.WriteString(analyzer.Name);
                        w.WriteComma();

                        w.WritePropertyName(nameof(AnalyzerDefinition.Code));
                        w.WriteString(analyzer.Code);

                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }
    }
}
