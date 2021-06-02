using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Analyzers
{
    public class AnalyzersHandler : ServerRequestHandler
    {
        [RavenAction("/analyzers", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var analyzers = new Dictionary<string, AnalyzerDefinition>();

                foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, PutServerWideAnalyzerCommand.Prefix, 0, long.MaxValue))
                {
                    var analyzerDefinition = JsonDeserializationServer.AnalyzerDefinition(item.Value);

                    analyzers.Add(item.ItemName.Substring(PutServerWideAnalyzerCommand.Prefix.Length), analyzerDefinition);
                }

                var namesOnly = GetBoolValueQueryString("namesOnly", false) ?? false;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
}
