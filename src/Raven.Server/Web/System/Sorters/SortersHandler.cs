using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Sorters
{
    public class SortersHandler : ServerRequestHandler
    {
        [RavenAction("/sorters", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var sorters = new Dictionary<string, SorterDefinition>();

                foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, PutServerWideSorterCommand.Prefix, 0, long.MaxValue))
                {
                    var sorterDefinition = JsonDeserializationServer.SorterDefinition(item.Value);

                    sorters.Add(PutServerWideSorterCommand.ExtractName(item.ItemName), sorterDefinition);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Sorters", sorters.Values, (w, c, analyzer) =>
                    {
                        w.WriteStartObject();

                        w.WritePropertyName(nameof(SorterDefinition.Name));
                        w.WriteString(analyzer.Name);

                        w.WriteComma();
                        w.WritePropertyName(nameof(SorterDefinition.Code));
                        w.WriteString(analyzer.Code);

                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }
        }
    }
}
